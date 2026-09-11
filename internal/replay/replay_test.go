package replay_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/aditya0si/event-stream-platform/internal/platform/broker"
	"github.com/aditya0si/event-stream-platform/internal/platform/db"
	"github.com/aditya0si/event-stream-platform/internal/replay"
	"github.com/aditya0si/event-stream-platform/internal/sink"
	"github.com/aditya0si/event-stream-platform/internal/testsupport"
)

// These tests run against a real Postgres and — for the republish path — a real broker. They
// fail rather than skip when a dependency is absent, for the same reason the rest of the suite
// does: a skipped test asserts nothing about the behaviour it names.

func newStore(t *testing.T) (*replay.Store, *sink.Store, *pgxpool.Pool) {
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

	// Every test here writes dead letters, and the sink package asserts on the queue's depth as
	// a whole number. Writing without the lock would break that package's assertions while both
	// packages were individually correct — which is exactly what happened when these tests were
	// added. See testsupport.LockDLQ.
	testsupport.LockDLQ(t, pool)

	store, err := replay.NewStore(pool)
	if err != nil {
		t.Fatalf("replay.NewStore: %v", err)
	}
	sinkStore, err := sink.NewStore(pool, "test-consumer/replay")
	if err != nil {
		t.Fatalf("sink.NewStore: %v", err)
	}
	return store, sinkStore, pool
}

// deadLetter writes a refusal through the real writer, so the tests exercise the same insert
// path the consumer uses rather than a hand-built row that could differ.
func deadLetter(t *testing.T, sinkStore *sink.Store, eventID, reason string, payload []byte) {
	t.Helper()

	if err := sinkStore.RecordDeadLetter(context.Background(), sink.DeadLetter{
		EventID:   eventID,
		Key:       "v-replay",
		Payload:   payload,
		Error:     "the failure this entry records",
		Reason:    reason,
		Attempts:  1,
		Topic:     "telemetry.raw.v1",
		Partition: 3,
		Offset:    4242,
	}); err != nil {
		t.Fatalf("RecordDeadLetter(%s): %v", eventID, err)
	}
}

func TestStore_ListsAndFiltersByState(t *testing.T) {
	store, sinkStore, _ := newStore(t)
	ctx := context.Background()

	deadID := uuid.Must(uuid.NewV7()).String()
	replayedID := uuid.Must(uuid.NewV7()).String()
	deadLetter(t, sinkStore, deadID, "decode", []byte(`{"broken":`))
	deadLetter(t, sinkStore, replayedID, "retries_exhausted", []byte(`{"ok":true}`))

	if _, err := store.MarkReplayed(ctx, replayedID); err != nil {
		t.Fatalf("MarkReplayed: %v", err)
	}

	dead, err := store.List(ctx, replay.Filter{State: replay.StateDead, Limit: 500})
	if err != nil {
		t.Fatalf("List(dead): %v", err)
	}
	if !contains(dead, deadID) {
		t.Error("the dead entry is missing from the dead listing")
	}
	if contains(dead, replayedID) {
		t.Error("a replayed entry appeared in the dead listing: the filter is not applied")
	}

	replayed, err := store.List(ctx, replay.Filter{State: replay.StateReplayed, Limit: 500})
	if err != nil {
		t.Fatalf("List(replayed): %v", err)
	}
	if !contains(replayed, replayedID) {
		t.Error("the replayed entry is missing from the replayed listing")
	}

	all, err := store.List(ctx, replay.Filter{State: "", Limit: 500})
	if err != nil {
		t.Fatalf("List(all): %v", err)
	}
	if !contains(all, deadID) || !contains(all, replayedID) {
		t.Error("the unfiltered listing must contain both entries")
	}
}

// TestStore_MarkReplayedHasExactlyOneWinner is the concurrency control: two operators
// replaying the same event must not both be told they changed it.
func TestStore_MarkReplayedHasExactlyOneWinner(t *testing.T) {
	store, sinkStore, _ := newStore(t)

	id := uuid.Must(uuid.NewV7()).String()
	deadLetter(t, sinkStore, id, "decode", []byte(`{}`))

	const workers = 8
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		winners int
		fails   []error
	)
	wg.Add(workers)
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			<-start
			won, err := store.MarkReplayed(context.Background(), id)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				fails = append(fails, err)
				return
			}
			if won {
				winners++
			}
		}()
	}
	close(start)
	wg.Wait()

	if len(fails) > 0 {
		t.Fatalf("concurrent MarkReplayed failed: %v", fails[0])
	}
	if winners != 1 {
		t.Errorf("winners = %d, want exactly 1: the `AND state = 'dead'` predicate must make "+
			"this an atomic claim, not a race", winners)
	}
}

// TestStore_ReplayStateIsNotSticky is the regression test for a defect found while writing the
// replay path: RecordDeadLetter did not reset `state`, so an event that was replayed and then
// failed *again* reported as 'replayed' forever. The queue would look handled while the event
// sat there unprocessed — the exact shape of failure this project keeps hunting for.
func TestStore_ReplayStateIsNotSticky(t *testing.T) {
	store, sinkStore, _ := newStore(t)
	ctx := context.Background()

	id := uuid.Must(uuid.NewV7()).String()
	deadLetter(t, sinkStore, id, "retries_exhausted", []byte(`{"retry":"me"}`))

	won, err := store.MarkReplayed(ctx, id)
	if err != nil || !won {
		t.Fatalf("MarkReplayed: won=%v err=%v", won, err)
	}

	// The replay did not fix it, so the consumer refuses it again.
	deadLetter(t, sinkStore, id, "retries_exhausted", []byte(`{"retry":"me"}`))

	entry, err := store.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if entry.State != replay.StateDead {
		t.Errorf("state = %q after a failed replay, want %q: an event that failed again is "+
			"awaiting attention again, and reporting it as replayed hides it from the operator",
			entry.State, replay.StateDead)
	}
	if entry.ReplayedAt != nil {
		t.Errorf("replayed_at = %v, want NULL: the columns must move together, or the CHECK "+
			"constraint and the state disagree", *entry.ReplayedAt)
	}
}

func TestStore_GetReportsAMissingEntry(t *testing.T) {
	store, _, _ := newStore(t)

	_, err := store.Get(context.Background(), uuid.Must(uuid.NewV7()).String())
	if !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("Get on an unknown id returned %v, want pgx.ErrNoRows so the caller can tell "+
			"'no such entry' from 'the database is unavailable'", err)
	}
}

func TestReplayer_RefusesAnEntryThatWasAlreadyReplayed(t *testing.T) {
	store, sinkStore, _ := newStore(t)
	ctx := context.Background()

	id := uuid.Must(uuid.NewV7()).String()
	deadLetter(t, sinkStore, id, "decode", []byte(`{"already":"done"}`))
	if _, err := store.MarkReplayed(ctx, id); err != nil {
		t.Fatalf("MarkReplayed: %v", err)
	}

	// A publisher is required by the constructor but must never be reached on this path, so
	// the test asserts the refusal without a broker — and the fake records any call, which
	// would prove the guard failed.
	fake := &recordingPublisher{}
	rep, err := replay.NewReplayer(store, fake)
	if err != nil {
		t.Fatalf("NewReplayer: %v", err)
	}

	if _, err := rep.Replay(ctx, id); err == nil {
		t.Fatal("Replay accepted an entry that had already been replayed")
	}
	if len(fake.calls) != 0 {
		t.Errorf("the entry was published %d time(s) despite being already replayed", len(fake.calls))
	}
}

func TestReplayer_RefusesWithoutAnEntry(t *testing.T) {
	store, _, _ := newStore(t)

	rep, err := replay.NewReplayer(store, &recordingPublisher{})
	if err != nil {
		t.Fatalf("NewReplayer: %v", err)
	}
	if _, err := rep.Replay(context.Background(), uuid.Must(uuid.NewV7()).String()); err == nil {
		t.Fatal("Replay reported success for an event that does not exist")
	}
}

// TestReplayer_RepublishesTheOriginalBytesWithProvenance is the wire test: the payload must
// come back byte-for-byte, and the headers must say where it came from and that it is a replay.
func TestReplayer_RepublishesTheOriginalBytesWithProvenance(t *testing.T) {
	seeds := testsupport.RequireBroker(t)
	store, sinkStore, _ := newStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	topic := testsupport.CreateTopic(t, seeds, "telemetry.test.replay", 1)

	id := uuid.Must(uuid.NewV7()).String()
	// Deliberately not valid JSON: the payload that reaches the DLQ usually cannot be parsed,
	// so byte fidelity is the property that matters most here.
	payload := []byte(`{not valid json — and that is the point`)
	if err := sinkStore.RecordDeadLetter(ctx, sink.DeadLetter{
		EventID:   id,
		Key:       "v-e2e",
		Payload:   payload,
		Error:     "decode failed",
		Reason:    "decode",
		Attempts:  3,
		Topic:     topic,
		Partition: 0,
		Offset:    17,
	}); err != nil {
		t.Fatalf("RecordDeadLetter: %v", err)
	}

	pub, err := broker.NewProducer(ctx, broker.Options{SeedBrokers: seeds, ClientID: "esp-test-replayer"})
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer pub.Close()

	rep, err := replay.NewReplayer(store, pub)
	if err != nil {
		t.Fatalf("NewReplayer: %v", err)
	}

	res, err := rep.Replay(ctx, id)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if !res.Marked {
		t.Error("Marked = false on the first replay of a dead entry")
	}

	// Read it back off the broker.
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.ClientID("esp-test-replay-reader"),
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	defer consumer.Close()

	var got *kgo.Record
	deadline := time.Now().Add(45 * time.Second)
	for got == nil && time.Now().Before(deadline) {
		fetches := consumer.PollRecords(ctx, 10)
		fetches.EachRecord(func(r *kgo.Record) {
			if got == nil {
				got = r
			}
		})
	}
	if got == nil {
		t.Fatal("the replayed record never appeared on the topic")
	}

	if string(got.Value) != string(payload) {
		t.Errorf("payload = %q, want the original bytes %q: replay must not normalise the event "+
			"that failed", got.Value, payload)
	}
	if string(got.Key) != id {
		t.Errorf("key = %q, want the event id %q", got.Key, id)
	}

	headers := map[string]string{}
	for _, h := range got.Headers {
		headers[h.Key] = string(h.Value)
	}
	for k, want := range map[string]string{
		"x-replay":           "true",
		"x-origin-topic":     topic,
		"x-origin-partition": "0",
		"x-origin-offset":    "17",
		"x-origin-reason":    "decode",
	} {
		if headers[k] != want {
			t.Errorf("header %s = %q, want %q (headers seen: %v)", k, headers[k], want, headers)
		}
	}
	if !strings.Contains(headers["x-origin-error"], "decode failed") {
		t.Errorf("x-origin-error = %q, want the original failure text", headers["x-origin-error"])
	}

	// And the row is marked, so a second run does not repeat it.
	entry, err := store.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if entry.State != replay.StateReplayed {
		t.Errorf("state = %q after a successful replay, want %q", entry.State, replay.StateReplayed)
	}
}

// recordingPublisher counts calls and remembers what it was asked to publish, so a test can
// assert that a guard *prevented* a publish rather than merely that no error was returned.
type recordingPublisher struct {
	calls []broker.Record
}

func (p *recordingPublisher) Produce(_ context.Context, _ string, recs []broker.Record) error {
	p.calls = append(p.calls, recs...)
	return nil
}

func contains(entries []replay.Entry, id string) bool {
	for _, e := range entries {
		if e.EventID == id {
			return true
		}
	}
	return false
}
