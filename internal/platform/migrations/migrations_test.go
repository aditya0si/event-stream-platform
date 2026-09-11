package migrations

import (
	"regexp"
	"strings"
	"testing"
)

// TestEmbeddedMigrationsArePresent covers a failure mode that is otherwise invisible: if
// the //go:embed pattern matches nothing, the binary starts happily and applies zero
// migrations — indistinguishable at runtime from an already-current database. The only
// thing that catches it is a test that asserts the files were actually compiled in.
func TestEmbeddedMigrationsArePresent(t *testing.T) {
	ms, err := load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(ms) == 0 {
		t.Fatal("no migrations are embedded: the //go:embed pattern matched nothing")
	}
	if ms[0].version != "0001" {
		t.Fatalf("first migration is %q, want 0001", ms[0].version)
	}
}

func TestMigrationsAreOrderedAndWellFormed(t *testing.T) {
	ms, err := load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	for i, m := range ms {
		if len(m.version) != 4 {
			t.Errorf("%q: version must be four digits so lexical order is chronological order", m.name)
		}
		if m.name == "" {
			t.Errorf("%s: migration has no description after the version prefix", m.version)
		}
		if strings.TrimSpace(m.body) == "" {
			t.Errorf("%s_%s has an empty body", m.version, m.name)
		}
		if i > 0 && ms[i-1].version >= m.version {
			t.Errorf("migrations are not in ascending order: %s then %s", ms[i-1].version, m.version)
		}
	}
}

// TestSchemaCarriesTheDesignsMechanisms asserts that the constraints the design's
// guarantees rest on are actually in the DDL. Each of these is load-bearing rather than
// cosmetic: losing one would quietly remove a guarantee the README claims.
func TestSchemaCarriesTheDesignsMechanisms(t *testing.T) {
	ms, err := load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	var b strings.Builder
	for _, m := range ms {
		b.WriteString(m.body)
	}
	sql := b.String()

	for _, want := range []string{
		"CREATE TABLE vehicle_positions",
		"CREATE TABLE vehicle_current",
		"CREATE TABLE processed_events",
		"CREATE TABLE dead_letters",
		"CREATE TABLE schema_versions",
		// A dead letter must not be able to be in an ambiguous state (ADR-002's DLQ path).
		"CHECK (state IN ('dead', 'replayed'))",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("the schema does not contain %q", want)
		}
	}

	// The deduplication mechanism, checked inside the table that owns it — a bare
	// "event_id uuid PRIMARY KEY" would also match vehicle_positions and prove nothing
	// about the idempotency claim.
	idx := strings.Index(sql, "CREATE TABLE processed_events")
	if idx < 0 {
		t.Fatal("processed_events is missing from the schema")
	}
	block := sql[idx:]
	if end := strings.Index(block, ");"); end > 0 {
		block = block[:end]
	}
	if !regexp.MustCompile(`event_id\s+uuid\s+PRIMARY KEY`).MatchString(block) {
		t.Error("processed_events.event_id is not a PRIMARY KEY: that constraint is the " +
			"mechanism behind the effectively-once claim (ADR-002, ADR-006), not a formality")
	}
}
