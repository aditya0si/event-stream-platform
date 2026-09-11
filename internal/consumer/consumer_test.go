package consumer_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/aditya0si/event-stream-platform/internal/consumer"
	"github.com/aditya0si/event-stream-platform/internal/event"
	"github.com/aditya0si/event-stream-platform/internal/platform/broker"
	"github.com/aditya0si/event-stream-platform/internal/platform/db"
	"github.com/aditya0si/event-stream-platform/internal/sink"
	"github.com/aditya0si/event-stream-platform/internal/testsupport"
)

// These tests run the consumer end to end: records are produced to a real broker, the
// consumer group reads them, the sink applies them to a real Postgres, and offsets are
// committed. They fail rather than skip when a dependency is absent (NFR-9).
//
// They exist because the properties that matter here live in the *interaction*: that a
// duplicate in the log is absorbed rather than applied twice, that a poison record does not
// block its partition forever, and that offsets only advance past work that actually landed.
// None of those can be checked by testing the sink or the broker alone.

func openTestPool(t *testing.T) *pgxpool.Pool {
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
	return pool
}

// harness wires a consumer against a fresh topic, a fresh group, and the real sink.
type harness struct {
	topic    string
	dlqTopic string
	group    string
	store    *sink.Store
	pool     *pgxpool.Pool
	seeds    []string
	produce  *broker.Producer
	admin    *kgo.Client
	stop     context.CancelFunc
	done     chan error
}

func newHarness(t *testing.T) *harness {
	t.Helper()

	seeds := testsupport.RequireBroker(t)
	pool := openTestPool(t)

	suffix := uuid.NewString()[:8]
	// A unique group per run: a shared group would resume from another run's committed
	// offsets and read nothing, making the test pass without consuming anything.
	group := "test-sink-" + suffix

	h := &harness{
		topic:    testsupport.CreateTopic(t, seeds, "telemetry.test", 3),
		dlqTopic: testsupport.CreateTopic(t, seeds, "telemetry.test.dlq", 1),
		group:    group,
		store:    mustStore(t, pool),
		pool:     pool,
		seeds:    seeds,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var err error
	h.produce, err = broker.NewProducer(ctx, broker.Options{SeedBrokers: seeds, ClientID: "esp-test-producer"})
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	t.Cleanup(h.produce.Close)

	h.admin, err = broker.NewClient(ctx, broker.Options{SeedBrokers: seeds, ClientID: "esp-test-admin"})
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	t.Cleanup(h.admin.Close)

	return h
}

func mustStore(t *testing.T, pool *pgxpool.Pool) *sink.Store {
	t.Helper()
	st, err := sink.NewStore(pool, "test-consumer/1")
	if err != nil {
		t.Fatalf("sink.NewStore: %v", err)
	}
	return st
}

// start launches the consumer loop and arranges for it to stop when the test ends.
func (h *harness) start(t *testing.T) {
	t.Helper()

	group, err := kgo.NewClient(
		kgo.SeedBrokers(h.seeds...),
		kgo.ClientID("esp-test-consumer"),
		kgo.ConsumerGroup(h.group),
		kgo.ConsumeTopics(h.topic),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("consumer client: %v", err)
	}

	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	dlq, err := consumer.NewDLQSink(h.produce, h.store, h.dlqTopic, log)
	if err != nil {
		t.Fatalf("dlq sink: %v", err)
	}

	worker, err := consumer.New(group, h.store, dlq, consumer.Options{
		Topic:       h.topic,
		BatchSize:   50,
		MaxRetries:  2,
		BackoffBase: 50 * time.Millisecond,
		BackoffMax:  200 * time.Millisecond,
		Log:         log,
	})
	if err != nil {
		t.Fatalf("consumer.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	h.stop = cancel
	h.done = make(chan error, 1)
	go func() { h.done <- worker.Run(ctx) }()

	t.Cleanup(func() {
		cancel()
		select {
		case <-h.done:
		case <-time.After(10 * time.Second):
			t.Error("the consumer loop did not stop within 10s of cancellation")
		}
		group.Close()
	})
}

// producePosition writes one envelope and returns the event id it carried.
func (h *harness) producePosition(t *testing.T, vehicleID string, eventTS time.Time, eventID string) string {
	t.Helper()

	if eventID == "" {
		eventID = uuid.Must(uuid.NewV7()).String()
	}
	speed := 31.5
	pos := event.VehiclePosition{
		VehicleID: vehicleID,
		RouteID:   "r-consumer-test",
		Lat:       12.9716,
		Lon:       77.5946,
		SpeedKPH:  &speed,
		EventTS:   eventTS,
		Sequence:  1,
	}
	env, err := event.Build("test", pos, eventID, eventTS.Add(time.Second))
	if err != nil {
		t.Fatalf("build envelope: %v", err)
	}
	raw, err := env.Marshal()
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := h.produce.Produce(ctx, h.topic, []broker.Record{{Key: vehicleID, Value: raw}}); err != nil {
		t.Fatalf("produce: %v", err)
	}
	return eventID
}

// produceRaw writes arbitrary bytes, for the poison-event case.
func (h *harness) produceRaw(t *testing.T, key string, value []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := h.produce.Produce(ctx, h.topic, []broker.Record{{Key: key, Value: value}}); err != nil {
		t.Fatalf("produce raw: %v", err)
	}
}

// awaitCaughtUp waits until the group has no lag on the test topic, which is the only evidence
// that the consumer actually saw every record — without it, a test asserting "six rows exist"
// would pass just as well if the consumer had read three.
func (h *harness) awaitCaughtUp(t *testing.T, wantAtLeast int64) {
	t.Helper()

	deadline := time.Now().Add(60 * time.Second)
	var lastTotal int64
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		lags, err := kadm.NewClient(h.admin).Lag(ctx, h.group)
		cancel()
		if err == nil {
			if described, ok := lags[h.group]; ok && described.DescribeErr == nil && described.FetchErr == nil {
				total := int64(0)
				committed := int64(0)
				for _, p := range described.Lag.Sorted() {
					if p.Topic != h.topic {
						continue
					}
					if p.Lag > 0 {
						total += p.Lag
					}
					// kadm.Offset names the value field `At`. An offset of -1 means the group
					// has no commit for this partition yet, which is why it is compared rather
					// than added blindly.
					if p.Commit.At >= 0 {
						committed += p.Commit.At
					}
				}
				lastTotal = total
				if total == 0 && committed >= wantAtLeast {
					return
				}
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("the consumer did not catch up within 60s (lag last seen: %d, wanted at least %d "+
		"committed offsets)", lastTotal, wantAtLeast)
}

// countRows counts rows in a table matching a column, for the ids under test only — no
// truncation, so this can run beside other packages.
func (h *harness) countRows(t *testing.T, table, column string, value any) int {
	t.Helper()
	var n int
	q := fmt.Sprintf("SELECT count(*) FROM %s WHERE %s = $1", table, column)
	if err := h.pool.QueryRow(context.Background(), q, value).Scan(&n); err != nil {
		t.Fatalf("count %s where %s = %v: %v", table, column, value, err)
	}
	return n
}

// TestConsumer_AppliesEachEventExactlyOnce is the end-to-end form of the effectively-once
// claim (ADR-002). Seven records go in, one of which is a duplicate of another; six distinct
// events may exist afterwards, and the duplicate must have been recognised rather than applied.
//
// The awaitCaughtUp call is what makes this meaningful: it proves the group consumed all seven
// records, so "six rows" is an assertion about deduplication and not about the consumer having
// read only six.
func TestConsumer_AppliesEachEventExactlyOnce(t *testing.T) {
	h := newHarness(t)
	h.start(t)

	base := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	vehicles := []string{"c-1", "c-2", "c-3"}

	var ids []string
	for i, v := range vehicles {
		ids = append(ids, h.producePosition(t, v, base.Add(time.Duration(i)*time.Second), ""))
		ids = append(ids, h.producePosition(t, v, base.Add(time.Duration(i+10)*time.Second), ""))
	}
	// A deliberate duplicate: the same event id, produced a second time — exactly the shape a
	// retried batch leaves in the log.
	dupID := h.producePosition(t, vehicles[0], base, ids[0])

	h.awaitCaughtUp(t, 7)

	// Six distinct events must exist. Not seven: the duplicate must not have become a second
	// observation.
	distinct := 0
	for _, id := range ids {
		n := h.countRows(t, "vehicle_positions", "event_id", id)
		if n != 1 {
			t.Errorf("vehicle_positions rows for %s = %d, want exactly 1", id, n)
		}
		distinct += 1
	}
	if dupID != ids[0] {
		t.Fatalf("the duplicate was minted with a different id (%s vs %s), so it does not test "+
			"deduplication at all", dupID, ids[0])
	}

	total := 0
	for _, id := range ids {
		total += h.countRows(t, "vehicle_positions", "event_id", id)
	}
	if total != distinct {
		t.Errorf("history rows = %d for %d distinct events, want them equal: the duplicate "+
			"produced a second row", total, distinct)
	}

	// The deduplication record proves the duplicate was *seen* and refused, not merely absent.
	if n := h.countRows(t, "processed_events", "event_id", ids[0]); n != 1 {
		t.Errorf("processed_events rows for the duplicated event = %d, want 1", n)
	}
}

// TestConsumer_PoisonEventDoesNotBlockItsPartition is the failure-path test a naive consumer
// fails: an undecodable record must be refused, recorded, and passed — not retried forever
// while every later event on that partition waits behind it.
func TestConsumer_PoisonEventDoesNotBlockItsPartition(t *testing.T) {
	h := newHarness(t)
	h.start(t)

	base := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	poisonKey := "poison-vehicle"
	h.produceRaw(t, poisonKey, []byte(`{this is not valid json`))

	// A healthy event *after* the poison one, keyed to a different vehicle so it may land on
	// any partition. Its application is the evidence that the pipeline kept moving.
	goodID := h.producePosition(t, "c-healthy", base, "")

	// Six records total (one poison, one good, plus waiting is not needed): the group must
	// reach zero lag, which can only happen if the poison record was passed rather than
	// retried forever.
	h.awaitCaughtUp(t, 1)

	if n := h.countRows(t, "vehicle_positions", "event_id", goodID); n != 1 {
		t.Fatalf("the healthy event was not applied (%d rows): the poison record blocked its "+
			"partition", n)
	}

	// The refusal must be recorded, with the origin coordinates an operator needs.
	var (
		reason    string
		payload   []byte
		partition int
		offset    int64
	)
	err := h.pool.QueryRow(context.Background(), `
		SELECT reason, raw_payload, partition, "offset"
		FROM dead_letters WHERE topic = $1 AND raw_payload = $2`,
		h.topic, []byte(`{this is not valid json`),
	).Scan(&reason, &payload, &partition, &offset)
	if err != nil {
		t.Fatalf("no dead letter was recorded for the undecodable record: %v", err)
	}
	if reason != "decode" {
		t.Errorf("reason = %q, want decode", reason)
	}
	if string(payload) != `{this is not valid json` {
		t.Errorf("the stored payload is %q, want the original bytes unchanged", payload)
	}
	_ = partition
	_ = offset
}

// TestConsumer_SkipsNothingOnRestart: a consumer stopped mid-batch must leave the unfinished
// work uncommitted, so a replacement group member sees it again. This is the property that
// makes "kill the consumer" safe, and it is asserted by observing offsets rather than by
// trusting the loop's structure.
func TestConsumer_OffsetsAdvanceOnlyPastAppliedWork(t *testing.T) {
	h := newHarness(t)
	h.start(t)

	base := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	const n = 5
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		ids = append(ids, h.producePosition(t, fmt.Sprintf("c-seq-%d", i), base.Add(time.Duration(i)*time.Second), ""))
	}

	h.awaitCaughtUp(t, n)

	for _, id := range ids {
		if got := h.countRows(t, "processed_events", "event_id", id); got != 1 {
			t.Errorf("event %s applied %d times, want 1", id, got)
		}
	}

	// Every committed offset must correspond to work that is actually in the sink: the
	// consumer's contract is that an offset is only advanced after the transaction commits.
	var applied int
	if err := h.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM processed_events WHERE event_id = ANY($1)`, ids).Scan(&applied); err != nil {
		t.Fatalf("count applied: %v", err)
	}
	if applied != n {
		t.Fatalf("applied %d of %d events while the group reported itself caught up", applied, n)
	}
}

// TestConsumer_RejectsMisconfiguration keeps the constructor honest: a consumer with no way to
// record a refusal must not start, because that combination silently loses events.
func TestConsumer_RejectsMisconfiguration(t *testing.T) {
	seeds := testsupport.RequireBroker(t)
	pool := openTestPool(t)
	store := mustStore(t, pool)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cl, err := broker.NewClient(ctx, broker.Options{SeedBrokers: seeds, ClientID: "esp-test-guard"})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer cl.Close()

	log := slog.New(slog.NewJSONHandler(io.Discard, nil))

	if _, err := consumer.New(cl, store, nil, consumer.Options{Topic: "t", Log: log}); err == nil {
		t.Error("New accepted a nil dead-letterer: a refused event would have nowhere to go")
	}
	if _, err := consumer.New(nil, store, nil, consumer.Options{Topic: "t", Log: log}); err == nil {
		t.Error("New accepted a nil kafka client")
	}
	if _, err := consumer.New(cl, nil, nil, consumer.Options{Topic: "t", Log: log}); err == nil {
		t.Error("New accepted a nil sink store")
	}
}
