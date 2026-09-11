// Package testsupport holds the helpers that connect tests to real dependencies.
//
// Its one job is to make a missing dependency a loud failure rather than a silent skip.
// NFR-9 in docs/DESIGN.md states the rule: the suite runs against a real Postgres, a real
// Redis, and a real broker, and a skipped test is worse than a red one — a green suite that
// quietly measured nothing is the most expensive kind of false confidence.
package testsupport

import (
	"os"
	"strings"
	"testing"
)

// RequireBroker returns the broker seed addresses, or fails the test.
//
// It fails rather than skips on purpose. The behaviour a test in this package verifies —
// partition assignment, offset behaviour, redelivery — cannot be answered by a fake, so a
// test that cannot reach a broker has not verified anything and must not report success.
func RequireBroker(t *testing.T) []string {
	t.Helper()

	raw := strings.TrimSpace(os.Getenv("TEST_BROKER_SEEDS"))
	if raw == "" {
		t.Fatalf("TEST_BROKER_SEEDS is not set.\n" +
			"These tests run against a real broker and fail rather than skip, because a skipped\n" +
			"broker test asserts nothing about partition assignment, offsets, or redelivery.\n" +
			"Start the stack with `docker compose up -d` and run:\n" +
			"    TEST_BROKER_SEEDS=localhost:19092 go test ./...")
	}

	var seeds []string
	for _, part := range strings.Split(raw, ",") {
		if p := strings.TrimSpace(part); p != "" {
			seeds = append(seeds, p)
		}
	}
	if len(seeds) == 0 {
		t.Fatalf("TEST_BROKER_SEEDS is set to %q but contains no addresses", raw)
	}
	return seeds
}

// RequireDatabase returns the test database URL, or fails the test, for the same reason and
// with the same wording.
func RequireDatabase(t *testing.T) string {
	t.Helper()

	url := strings.TrimSpace(os.Getenv("TEST_DATABASE_URL"))
	if url == "" {
		t.Fatalf("TEST_DATABASE_URL is not set.\n" +
			"These tests run against a real Postgres and fail rather than skip, because a skipped\n" +
			"database test asserts nothing about constraints, transactions, or transaction\n" +
			"visibility.\n" +
			"Start the stack with `docker compose up -d` and run:\n" +
			"    TEST_DATABASE_URL=postgres://esp:esp@localhost:5433/event_stream?sslmode=disable go test ./...")
	}
	return url
}

// RequireRedis returns the test Redis URL, or fails the test.
func RequireRedis(t *testing.T) string {
	t.Helper()

	url := strings.TrimSpace(os.Getenv("TEST_REDIS_URL"))
	if url == "" {
		t.Fatalf("TEST_REDIS_URL is not set.\n" +
			"These tests run against a real Redis and fail rather than skip.\n" +
			"Start the stack with `docker compose up -d` and run:\n" +
			"    TEST_REDIS_URL=redis://localhost:6380/1 go test ./...")
	}
	return url
}
