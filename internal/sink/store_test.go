package sink_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aditya0si/event-stream-platform/internal/event"
	"github.com/aditya0si/event-stream-platform/internal/platform/db"
	"github.com/aditya0si/event-stream-platform/internal/sink"
	"github.com/aditya0si/event-stream-platform/internal/testsupport"
)

// These tests run against a real Postgres. Every property asserted here — the uniqueness that
// absorbs duplicates, the guarded upsert that refuses late data, the transaction that keeps the
// deduplication record and its effects together — is a property of the database, and a fake
// would answer none of them. They fail rather than skip when TEST_DATABASE_URL is absent.
//
// No test truncates any table. Rows are identified by the event and vehicle ids the test mints
// for itself, so packages running in parallel cannot interfere and nothing has to be cleaned
// up for the assertions to hold.

func newStore(t *testing.T) (*sink.Store, *pgxpool.Pool) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := db.Open(ctx, db.Options{
		URL:            testsupport.RequireDatabase(t),
		MaxConns:       8,
		MinConns:       1,
		ConnectTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	t.Cleanup(pool.Close)

	// The dead-letter queue is shared state, and this package asserts on its depth as a whole
	// number. Other packages' tests run in parallel against the same database, so those
	// assertions are only meaningful while holding a cross-process lock — see
	// testsupport.LockDLQ, and the failure that prompted it.
	testsupport.LockDLQ(t, pool)

	st, err := sink.NewStore(pool, "test-consumer/1")
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	return st, pool
}

// input builds an Input for a vehicle, with a fresh event id unless one is given.
func input(t *testing.T, vehicleID string, eventTS time.Time, opts ...func(*sink.Input)) sink.Input {
	t.Helper()

	id := uuid.Must(uuid.NewV7()).String()
	speed := 30.0
	pos := event.VehiclePosition{
		VehicleID: vehicleID,
		RouteID:   "r-test",
		Lat:       12.9716,
		Lon:       77.5946,
		SpeedKPH:  &speed,
		EventTS:   eventTS,
		Sequence:  1,
	}
	env, err := event.Build("test", pos, id, eventTS.Add(time.Second))
	if err != nil {
		t.Fatalf("build envelope: %v", err)
	}

	in := sink.Input{Envelope: env, Position: pos, Partition: 0, Offset: 1}
	for _, o := range opts {
		o(&in)
	}
	return in
}

// countFor returns how many rows in table match the given column value.
func countFor(t *testing.T, pool *pgxpool.Pool, table, column, value string) int {
	t.Helper()

	var n int
	q := fmt.Sprintf("SELECT count(*) FROM %s WHERE %s = $1", table, column)
	if err := pool.QueryRow(context.Background(), q, value).Scan(&n); err != nil {
		t.Fatalf("count %s where %s = %q: %v", table, column, value, err)
	}
	return n
}

func TestApply_NewEventIsApplied(t *testing.T) {
	st, pool := newStore(t)
	ctx := context.Background()

	vehicle := "test-apply-" + uuid.NewString()[:8]
	in := input(t, vehicle, time.Now().UTC().Add(-time.Minute))

	outcome, err := st.Apply(ctx, in)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if outcome != sink.OutcomeApplied {
		t.Fatalf("outcome = %q, want %q", outcome, sink.OutcomeApplied)
	}

	if n := countFor(t, pool, "processed_events", "event_id", in.Envelope.EventID); n != 1 {
		t.Errorf("processed_events rows = %d, want 1", n)
	}
	if n := countFor(t, pool, "vehicle_positions", "event_id", in.Envelope.EventID); n != 1 {
		t.Errorf("vehicle_positions rows = %d, want 1", n)
	}

	cur, err := st.GetCurrent(ctx, vehicle)
	if err != nil {
		t.Fatalf("GetCurrent: %v", err)
	}
	if cur.LastEventID != in.Envelope.EventID {
		t.Errorf("current state points at %q, want %q", cur.LastEventID, in.Envelope.EventID)
	}
}

// TestApply_DuplicateIsAbsorbed is the test behind the effectively-once claim (ADR-002): a
// redelivered event produces no second side effect, and says so rather than failing.
func TestApply_DuplicateIsAbsorbed(t *testing.T) {
	st, pool := newStore(t)
	ctx := context.Background()

	vehicle := "test-dup-" + uuid.NewString()[:8]
	in := input(t, vehicle, time.Now().UTC().Add(-time.Minute))

	first, err := st.Apply(ctx, in)
	if err != nil {
		t.Fatalf("first Apply: %v", err)
	}
	if first != sink.OutcomeApplied {
		t.Fatalf("first outcome = %q, want applied", first)
	}

	// The same event again, with a different offset: this is exactly the shape a redelivery
	// after a crash takes — the same bytes at a new log position.
	in2 := in
	in2.Offset = in.Offset + 1
	second, err := st.Apply(ctx, in2)
	if err != nil {
		t.Fatalf("second Apply: %v", err)
	}
	if second != sink.OutcomeDuplicate {
		t.Fatalf("second outcome = %q, want duplicate: a redelivered event must be recognised, "+
			"not applied again", second)
	}

	if n := countFor(t, pool, "processed_events", "event_id", in.Envelope.EventID); n != 1 {
		t.Errorf("processed_events rows = %d, want 1", n)
	}
	if n := countFor(t, pool, "vehicle_positions", "event_id", in.Envelope.EventID); n != 1 {
		t.Errorf("vehicle_positions rows = %d after a duplicate, want 1: history is append-only "+
			"and a duplicate is not a new observation", n)
	}
}

// TestApply_ConcurrentDuplicatesApplyExactlyOnce drives the race the design is built for: the
// same event delivered to several workers at once. Without the primary key this is the case
// that double-counts; with it, exactly one caller may see "applied".
func TestApply_ConcurrentDuplicatesApplyExactlyOnce(t *testing.T) {
	st, pool := newStore(t)

	vehicle := "test-conc-" + uuid.NewString()[:8]
	in := input(t, vehicle, time.Now().UTC().Add(-time.Minute))

	const workers = 8
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		applied  int
		dupes    int
		failures []error
	)
	wg.Add(workers)
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		go func(i int) {
			defer wg.Done()
			<-start // release them together, so the inserts genuinely collide
			each := in
			each.Offset = int64(i)
			outcome, err := st.Apply(context.Background(), each)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err != nil:
				failures = append(failures, err)
			case outcome == sink.OutcomeApplied:
				applied++
			case outcome == sink.OutcomeDuplicate:
				dupes++
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if len(failures) > 0 {
		t.Fatalf("%d of %d concurrent applies failed: %v", len(failures), workers, failures[0])
	}
	if applied != 1 {
		t.Errorf("applied = %d, want exactly 1: %d workers delivered the same event and only one "+
			"may count it", applied, workers)
	}
	if dupes != workers-1 {
		t.Errorf("duplicates = %d, want %d", dupes, workers-1)
	}
	if n := countFor(t, pool, "vehicle_positions", "event_id", in.Envelope.EventID); n != 1 {
		t.Errorf("vehicle_positions rows = %d, want 1", n)
	}
}

// TestApply_LateEventIsKeptButDoesNotMoveCurrentState is ADR-005 as an executable assertion:
// history keeps every accepted observation, current state is monotonic in event time.
func TestApply_LateEventIsKeptButDoesNotMoveCurrentState(t *testing.T) {
	st, pool := newStore(t)
	ctx := context.Background()

	vehicle := "test-late-" + uuid.NewString()[:8]
	now := time.Now().UTC().Add(-time.Minute)

	newer := input(t, vehicle, now)
	newer.Position.Lat, newer.Position.Lon = 12.99, 77.99
	newerEnv, err := event.Build("test", newer.Position, newer.Envelope.EventID, now.Add(time.Second))
	if err != nil {
		t.Fatalf("build newer: %v", err)
	}
	newer.Envelope = newerEnv

	if outcome, err := st.Apply(ctx, newer); err != nil || outcome != sink.OutcomeApplied {
		t.Fatalf("newer event: outcome=%q err=%v, want applied", outcome, err)
	}

	// An older observation arrives afterwards — a buffered report uploaded late.
	older := input(t, vehicle, now.Add(-10*time.Minute))
	older.Position.Lat, older.Position.Lon = 1.11, 2.22
	olderEnv, err := event.Build("test", older.Position, older.Envelope.EventID, now)
	if err != nil {
		t.Fatalf("build older: %v", err)
	}
	older.Envelope = olderEnv

	outcome, err := st.Apply(ctx, older)
	if err != nil {
		t.Fatalf("late Apply: %v", err)
	}
	if outcome != sink.OutcomeLate {
		t.Fatalf("outcome = %q, want %q: an event not newer than current state is late, not an error",
			outcome, sink.OutcomeLate)
	}

	// Kept in the history: the record of what a vehicle reported is append-only, and
	// discarding a late report would corrupt it to protect a derived view.
	if n := countFor(t, pool, "vehicle_positions", "event_id", older.Envelope.EventID); n != 1 {
		t.Errorf("history rows for the late event = %d, want 1", n)
	}

	cur, err := st.GetCurrent(ctx, vehicle)
	if err != nil {
		t.Fatalf("GetCurrent: %v", err)
	}
	if cur.LastEventID != newer.Envelope.EventID {
		t.Errorf("current state points at %q, want the newer event %q: a late event must not "+
			"move the map backwards", cur.LastEventID, newer.Envelope.EventID)
	}
	if cur.Lat != 12.99 {
		t.Errorf("current lat = %v, want the newer value 12.99", cur.Lat)
	}
}

// TestApply_RecordsProvenance checks the fields that make "where did this come from?" a query
// rather than an investigation.
func TestApply_RecordsProvenance(t *testing.T) {
	st, pool := newStore(t)

	vehicle := "test-prov-" + uuid.NewString()[:8]
	in := input(t, vehicle, time.Now().UTC().Add(-time.Minute),
		func(i *sink.Input) { i.Partition = 4; i.Offset = 987654 })

	if _, err := st.Apply(context.Background(), in); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	var (
		partition int
		offset    int64
	)
	if err := pool.QueryRow(context.Background(),
		`SELECT partition, "offset" FROM vehicle_positions WHERE event_id = $1`,
		in.Envelope.EventID).Scan(&partition, &offset); err != nil {
		t.Fatalf("read provenance: %v", err)
	}
	if partition != 4 || offset != 987654 {
		t.Errorf("history provenance = partition %d offset %d, want 4 and 987654", partition, offset)
	}

	if err := pool.QueryRow(context.Background(),
		`SELECT partition, "offset" FROM processed_events WHERE event_id = $1`,
		in.Envelope.EventID).Scan(&partition, &offset); err != nil {
		t.Fatalf("read processed provenance: %v", err)
	}
	if partition != 4 || offset != 987654 {
		t.Errorf("processed provenance = partition %d offset %d, want 4 and 987654", partition, offset)
	}
}

// TestApply_FailedEffectLeavesNoDeduplicationRecord is the atomicity test, and the one that
// would catch the worst possible regression: a deduplication record that commits while its
// effect rolls back. That combination marks an event as handled while losing it forever, and
// nothing downstream could ever notice.
//
// The failure is forced by writing a position the database refuses (a CHECK constraint on
// latitude) while bypassing the application's own validation, which is what a future caller
// with a bug would do.
func TestApply_FailedEffectLeavesNoDeduplicationRecord(t *testing.T) {
	st, pool := newStore(t)
	ctx := context.Background()

	vehicle := "test-atomic-" + uuid.NewString()[:8]
	bad := input(t, vehicle, time.Now().UTC().Add(-time.Minute))
	bad.Position.Lat = 999 // outside the CHECK constraint, and never sent by a validated producer

	if _, err := st.Apply(ctx, bad); err == nil {
		t.Fatal("Apply accepted a position the database must refuse")
	}

	// Nothing may have survived the failed transaction — not the effect, not the record.
	if n := countFor(t, pool, "processed_events", "event_id", bad.Envelope.EventID); n != 0 {
		t.Fatalf("processed_events has %d row(s) for an event whose effect failed: the "+
			"deduplication record committed while its effect rolled back, so this event is now "+
			"permanently lost while appearing handled", n)
	}
	if n := countFor(t, pool, "vehicle_positions", "event_id", bad.Envelope.EventID); n != 0 {
		t.Errorf("vehicle_positions has %d row(s) for a failed apply, want 0", n)
	}

	// And the same event id, with a valid position, must still be applicable: if the failed
	// attempt had left a dedup record behind, this would report "duplicate" and the event
	// would be silently dropped.
	good := bad
	good.Position.Lat = 12.5
	goodEnv, err := event.Build("test", good.Position, good.Envelope.EventID, time.Now().UTC())
	if err != nil {
		t.Fatalf("build corrected event: %v", err)
	}
	good.Envelope = goodEnv

	outcome, err := st.Apply(ctx, good)
	if err != nil {
		t.Fatalf("Apply after a failed attempt: %v", err)
	}
	if outcome != sink.OutcomeApplied {
		t.Fatalf("outcome after a failed attempt = %q, want applied: the failed transaction must "+
			"not have left a deduplication record", outcome)
	}
}

// TestApply_GuardsHold keeps the constructor's guards honest: a store built wrongly should fail
// loudly rather than panic on a nil dereference inside a consumer's poll loop, where the panic
// would take down a process that is otherwise doing its job.
func TestApply_GuardsHold(t *testing.T) {
	if _, err := sink.NewStore(nil, "consumer"); err == nil {
		t.Error("NewStore accepted a nil pool")
	}

	_, pool := newStore(t)
	if _, err := sink.NewStore(pool, ""); err == nil {
		t.Error("NewStore accepted an empty consumer id: a duplicate could not then be attributed " +
			"to a process, which is the question an incident starts with")
	}
}
