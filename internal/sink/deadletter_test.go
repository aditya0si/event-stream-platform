package sink_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/aditya0si/event-stream-platform/internal/sink"
)

// TestRecordDeadLetter_StoresBytesThatAreNotJSON is the test for migration 0002's reason to
// exist. The column was jsonb, which cannot store a payload that failed to decode — and a
// payload that failed to decode is one of the two things this table is for. A jsonb column
// would refuse the row, the refusal could not be recorded, and the event would be retried
// forever while blocking its partition.
func TestRecordDeadLetter_StoresBytesThatAreNotJSON(t *testing.T) {
	st, pool := newStore(t)
	ctx := context.Background()

	id := uuid.Must(uuid.NewV7()).String()
	raw := []byte(`{this is not valid json`)
	if err := st.RecordDeadLetter(ctx, sink.DeadLetter{
		EventID:   id,
		Key:       "v-1",
		Payload:   raw,
		Error:     "envelope: malformed JSON: invalid character 't' looking for beginning of object key string",
		Reason:    "decode",
		Attempts:  1,
		Topic:     "telemetry.raw.v1",
		Partition: 2,
		Offset:    42,
	}); err != nil {
		t.Fatalf("RecordDeadLetter rejected bytes that are not JSON, which is the case it exists "+
			"for: %v", err)
	}

	// The bytes must come back exactly as they went in: a replayed event has to be the event
	// that failed, not a normalised version of it.
	var got []byte
	if err := pool.QueryRow(ctx,
		`SELECT raw_payload FROM dead_letters WHERE event_id = $1`, id).Scan(&got); err != nil {
		t.Fatalf("read payload: %v", err)
	}
	if string(got) != string(raw) {
		t.Errorf("payload round-tripped as %q, want %q", got, raw)
	}
}

// TestRecordDeadLetter_IsIdempotentOnRedelivery: a poison event that keeps returning must not
// raise a unique violation, because a violation would make the refusal unrecordable and the
// event uncommittable — the partition would never advance past it.
func TestRecordDeadLetter_IsIdempotentOnRedelivery(t *testing.T) {
	st, pool := newStore(t)
	ctx := context.Background()

	id := uuid.Must(uuid.NewV7()).String()
	dl := sink.DeadLetter{
		EventID:   id,
		Key:       "v-2",
		Payload:   []byte(`{"broken":`),
		Error:     "first failure",
		Reason:    "decode",
		Attempts:  1,
		Topic:     "telemetry.raw.v1",
		Partition: 1,
		Offset:    7,
	}
	if err := st.RecordDeadLetter(ctx, dl); err != nil {
		t.Fatalf("first record: %v", err)
	}

	dl.Error = "second failure"
	dl.Attempts = 2 // the consumer counts attempts since it started, not cumulatively
	if err := st.RecordDeadLetter(ctx, dl); err != nil {
		t.Fatalf("recording the same dead letter twice failed, which would block the partition: %v", err)
	}

	var (
		attempts int
		errText  string
		state    string
	)
	if err := pool.QueryRow(ctx,
		`SELECT attempts, error, state FROM dead_letters WHERE event_id = $1`, id,
	).Scan(&attempts, &errText, &state); err != nil {
		t.Fatalf("read dead letter: %v", err)
	}
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3 (the row accumulates attempts across redeliveries)", attempts)
	}
	if errText != "second failure" {
		t.Errorf("error = %q, want the most recent failure", errText)
	}
	if state != "dead" {
		t.Errorf("state = %q, want dead", state)
	}
}

// TestDLQDepth_ReportsZeroRatherThanNothing: the gauge must exist and read 0 when the queue is
// empty. A queue-depth signal that is absent exactly when the system is healthy is the mistake
// the sibling repository records in its own ADR, and this asserts the value rather than the
// series' existence so the distinction survives refactoring.
func TestDLQDepth_ReportsZeroRatherThanNothing(t *testing.T) {
	st, pool := newStore(t)
	ctx := context.Background()

	before, err := st.DLQDepth(ctx)
	if err != nil {
		t.Fatalf("DLQDepth: %v", err)
	}

	id := uuid.Must(uuid.NewV7()).String()
	if err := st.RecordDeadLetter(ctx, sink.DeadLetter{
		EventID: id, Key: "v-3", Payload: []byte(`{}`), Error: "e", Reason: "decode",
		Attempts: 1, Topic: "t", Partition: 0, Offset: 0,
	}); err != nil {
		t.Fatalf("RecordDeadLetter: %v", err)
	}

	after, err := st.DLQDepth(ctx)
	if err != nil {
		t.Fatalf("DLQDepth after insert: %v", err)
	}
	if after != before+1 {
		t.Errorf("depth moved from %d to %d, want +1", before, after)
	}

	// Marking it replayed must remove it from the count: the number answers "how much is
	// waiting for an operator", not "how much has ever failed".
	if _, err := pool.Exec(ctx,
		`UPDATE dead_letters SET state = 'replayed', replayed_at = now() WHERE event_id = $1`, id,
	); err != nil {
		t.Fatalf("mark replayed: %v", err)
	}
	final, err := st.DLQDepth(ctx)
	if err != nil {
		t.Fatalf("DLQDepth after replay: %v", err)
	}
	if final != before {
		t.Errorf("depth after replay = %d, want %d (a replayed row is no longer awaiting attention)",
			final, before)
	}
}

// TestRecordDeadLetter_RefusesAnEventWithNoIdentity: a dead letter that cannot be looked up is
// useless, so writing one is worse than refusing — and the consumer derives an identity from
// the record's position precisely so this never happens in practice.
func TestRecordDeadLetter_RefusesAnEventWithNoIdentity(t *testing.T) {
	st, _ := newStore(t)

	if err := st.RecordDeadLetter(context.Background(), sink.DeadLetter{
		Payload: []byte(`{}`), Error: "e", Reason: "decode", Topic: "t",
	}); err == nil {
		t.Fatal("RecordDeadLetter accepted an event with no identifier: nothing could list or replay it")
	}
}

// TestRecordDeadLetter_RejectsAnUnknownReason keeps the reason vocabulary honest. The column
// has a CHECK constraint naming the four values the metrics use, so a typo is a defect rather
// than a new category nobody queries.
func TestRecordDeadLetter_RejectsAnUnknownReason(t *testing.T) {
	st, _ := newStore(t)

	err := st.RecordDeadLetter(context.Background(), sink.DeadLetter{
		EventID: uuid.Must(uuid.NewV7()).String(),
		Payload: []byte(`{}`),
		Error:   "e",
		Reason:  "because_i_said_so",
		Topic:   "t",
	})
	if err == nil {
		t.Fatal("an unrecognised reason was accepted, so a typo would silently become a category")
	}
}
