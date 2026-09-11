package config_test

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aditya0si/event-stream-platform/internal/platform/config"
)

// envKeys is every variable Load reads.
//
// Tests start from all of them unset so that a developer's shell, a CI runner, or a stray
// .env cannot make a test pass or fail for a reason the test does not control. The sibling
// project learned this the expensive way: an inherited DATABASE_URL pointed a suite at the
// wrong database and every test still reported green.
var envKeys = []string{
	"APP_ENV", "LOG_LEVEL",

	"DATABASE_URL", "DB_MAX_CONNS", "DB_MIN_CONNS", "DB_MAX_CONN_LIFETIME", "DB_CONNECT_TIMEOUT",

	"REDIS_URL", "REDIS_DIAL_TIMEOUT", "REDIS_READ_TIMEOUT", "REDIS_WRITE_TIMEOUT", "REDIS_CHANNEL",

	"BROKER_SEEDS", "BROKER_RAW_TOPIC", "BROKER_DLQ_TOPIC",
	"BROKER_RAW_PARTITIONS", "BROKER_DLQ_PARTITIONS", "BROKER_RETENTION", "BROKER_DLQ_RETENTION",

	"INGEST_ADDR", "INGEST_SHUTDOWN_TIMEOUT", "INGEST_READ_TIMEOUT", "INGEST_WRITE_TIMEOUT",
	"INGEST_IDLE_TIMEOUT", "INGEST_MAX_BODY_BYTES", "INGEST_MAX_BATCH", "INGEST_API_KEY",

	"CONSUMER_GROUP", "CONSUMER_METRICS_ADDR", "CONSUMER_BATCH_SIZE", "CONSUMER_COMMIT_INTERVAL",
	"CONSUMER_SHUTDOWN_TIMEOUT", "CONSUMER_MAX_RETRIES", "CONSUMER_RETRY_BACKOFF_BASE",
	"CONSUMER_RETRY_BACKOFF_MAX",

	"GATEWAY_ADDR", "GATEWAY_CLIENT_BUFFER", "GATEWAY_REPLAY_WINDOW", "GATEWAY_MAX_CLIENTS",
	"GATEWAY_SHUTDOWN_TIMEOUT", "GATEWAY_READ_TIMEOUT", "GATEWAY_WRITE_TIMEOUT",
}

// hermetic unsets every variable Load reads and restores the environment afterwards.
func hermetic(t *testing.T) {
	t.Helper()
	for _, k := range envKeys {
		old, had := os.LookupEnv(k)
		if err := os.Unsetenv(k); err != nil {
			t.Fatalf("unset %s: %v", k, err)
		}
		t.Cleanup(func() {
			if had {
				_ = os.Setenv(k, old)
				return
			}
			_ = os.Unsetenv(k)
		})
	}
}

// minimal returns the smallest environment Load accepts, for tests that are about some
// other setting.
func minimal(t *testing.T) {
	t.Helper()
	hermetic(t)
	t.Setenv("DATABASE_URL", "postgres://esp:esp@localhost:5433/event_stream?sslmode=disable")
}

func TestLoad_RequiresDatabaseURL(t *testing.T) {
	hermetic(t)
	_, err := config.Load()
	if err == nil {
		t.Fatal("Load succeeded with no DATABASE_URL: a process with no database has nothing to do and must not start")
	}
	if !strings.Contains(err.Error(), "DATABASE_URL") {
		t.Fatalf("the error does not name the missing variable: %v", err)
	}
}

func TestLoad_ReportsEveryProblemInOnePass(t *testing.T) {
	hermetic(t)
	// Three independent problems at once: a missing required value, an unparseable level,
	// and a semantically impossible pool size.
	t.Setenv("LOG_LEVEL", "verbose")
	t.Setenv("DB_MAX_CONNS", "2")
	t.Setenv("DB_MIN_CONNS", "5")

	_, err := config.Load()
	if err == nil {
		t.Fatal("Load accepted an impossible configuration")
	}
	msg := err.Error()
	for _, want := range []string{"DATABASE_URL", "LOG_LEVEL", "DB_MIN_CONNS"} {
		if !strings.Contains(msg, want) {
			t.Errorf("the combined error does not mention %s — an operator would have to fix the configuration one restart at a time:\n%s", want, msg)
		}
	}
}

func TestLoad_AppliesTheDesignsDefaults(t *testing.T) {
	minimal(t)
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	// The partition count is a design decision (DESIGN.md § D, ADR-005): consumer
	// parallelism is capped by it, and the scaling story in the README depends on the
	// number. Pinned here so it cannot drift unnoticed.
	if cfg.Broker.RawParts != 6 {
		t.Errorf("BROKER_RAW_PARTITIONS default = %d, want 6 (the design's partition count)", cfg.Broker.RawParts)
	}
	if cfg.Broker.DLQParts != 3 {
		t.Errorf("BROKER_DLQ_PARTITIONS default = %d, want 3", cfg.Broker.DLQParts)
	}

	// The per-connection buffer is the backpressure policy made concrete (ADR-004). A
	// default of zero would mean either an unbounded buffer or a client that is dropped on
	// its first event.
	if cfg.Gateway.ClientBuffer != 256 {
		t.Errorf("GATEWAY_CLIENT_BUFFER default = %d, want 256", cfg.Gateway.ClientBuffer)
	}

	if !cfg.IsDev() {
		t.Errorf("APP_ENV default = %q, want dev", cfg.Env)
	}
	if len(cfg.Broker.SeedBrokers) == 0 {
		t.Error("BROKER_SEEDS default produced no seed brokers")
	}
}

func TestLoad_ParsesDurationsFromTheEnvironment(t *testing.T) {
	minimal(t)
	t.Setenv("BROKER_RETENTION", "48h")
	t.Setenv("GATEWAY_REPLAY_WINDOW", "2m")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Broker.Retention != 48*time.Hour {
		t.Errorf("BROKER_RETENTION = %s, want 48h", cfg.Broker.Retention)
	}
	if cfg.Gateway.ReplayWindow != 2*time.Minute {
		t.Errorf("GATEWAY_REPLAY_WINDOW = %s, want 2m", cfg.Gateway.ReplayWindow)
	}
}

func TestLoad_RejectsBadDuration(t *testing.T) {
	minimal(t)
	t.Setenv("DB_CONNECT_TIMEOUT", "five seconds")
	_, err := config.Load()
	if err == nil {
		t.Fatal("Load accepted a duration that is not Go duration syntax")
	}
	if !strings.Contains(err.Error(), "DB_CONNECT_TIMEOUT") {
		t.Fatalf("the error does not name the variable: %v", err)
	}
}

func TestLoad_RejectsEmptyBrokerSeeds(t *testing.T) {
	minimal(t)
	// Set but containing nothing usable. This is the shape a templating mistake produces,
	// and it must not be treated as "unset, use the default".
	t.Setenv("BROKER_SEEDS", " , ")
	_, err := config.Load()
	if err == nil {
		t.Fatal("Load accepted a BROKER_SEEDS value with no brokers in it")
	}
	if !strings.Contains(err.Error(), "BROKER_SEEDS") {
		t.Fatalf("the error does not name the variable: %v", err)
	}
}

func TestLoad_RejectsBackoffMaxBelowBase(t *testing.T) {
	minimal(t)
	t.Setenv("CONSUMER_RETRY_BACKOFF_BASE", "5s")
	t.Setenv("CONSUMER_RETRY_BACKOFF_MAX", "1s")
	_, err := config.Load()
	if err == nil {
		t.Fatal("Load accepted a retry ceiling below its floor, which would make the backoff schedule meaningless")
	}
	if !strings.Contains(err.Error(), "CONSUMER_RETRY_BACKOFF_MAX") {
		t.Fatalf("the error does not name the variable: %v", err)
	}
}

func TestLoad_RejectsUnknownEnv(t *testing.T) {
	minimal(t)
	t.Setenv("APP_ENV", "staging")
	_, err := config.Load()
	if err == nil {
		t.Fatal("Load accepted an APP_ENV it does not implement")
	}
	if !strings.Contains(err.Error(), "APP_ENV") {
		t.Fatalf("the error does not name the variable: %v", err)
	}
}

func TestLoad_ProdRequiresIngestAPIKey(t *testing.T) {
	minimal(t)
	t.Setenv("APP_ENV", "prod")
	// No INGEST_API_KEY.
	_, err := config.Load()
	if err == nil {
		t.Fatal("Load allowed a production ingest endpoint with no authentication: it would accept telemetry from anyone who can reach the port")
	}
	if !strings.Contains(err.Error(), "INGEST_API_KEY") {
		t.Fatalf("the error does not name the variable: %v", err)
	}
}

func TestLoad_RejectsShortAPIKey(t *testing.T) {
	minimal(t)
	t.Setenv("INGEST_API_KEY", "tooshort")
	_, err := config.Load()
	if err == nil {
		t.Fatal("Load accepted an API key too short to be a meaningful secret")
	}
	if !strings.Contains(err.Error(), "16") {
		t.Errorf("the error should state the minimum length: %v", err)
	}
}

func TestLoad_AcceptsACompleteDevConfig(t *testing.T) {
	hermetic(t)
	t.Setenv("DATABASE_URL", "postgres://esp:esp@localhost:5433/event_stream?sslmode=disable")
	t.Setenv("REDIS_URL", "redis://localhost:6380/0")
	t.Setenv("BROKER_SEEDS", "localhost:19092,localhost:19093")
	t.Setenv("APP_ENV", "dev")
	t.Setenv("LOG_LEVEL", "debug")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load rejected a valid configuration: %v", err)
	}
	if len(cfg.Broker.SeedBrokers) != 2 {
		t.Errorf("SeedBrokers = %v, want two entries parsed from the comma-separated list", cfg.Broker.SeedBrokers)
	}
	if cfg.LogLevel.String() != "DEBUG" {
		t.Errorf("LogLevel = %s, want DEBUG", cfg.LogLevel)
	}
}

func TestLoad_ProdAcceptsAValidKey(t *testing.T) {
	minimal(t)
	t.Setenv("APP_ENV", "prod")
	t.Setenv("INGEST_API_KEY", "0123456789abcdef0123456789abcdef")
	if _, err := config.Load(); err != nil {
		t.Fatalf("Load rejected a valid production configuration: %v", err)
	}
}
