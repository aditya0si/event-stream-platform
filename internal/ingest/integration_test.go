package ingest_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/aditya0si/event-stream-platform/internal/event"
	"github.com/aditya0si/event-stream-platform/internal/ingest"
	"github.com/aditya0si/event-stream-platform/internal/platform/broker"
	"github.com/aditya0si/event-stream-platform/internal/platform/reqid"
	"github.com/aditya0si/event-stream-platform/internal/testsupport"
)

// This file tests the one thing the fake publisher in handler_test.go cannot: what actually
// lands on the broker.
//
// handler_test.go verifies the handler's *decisions* — which events are refused, what the
// response says, whether a failure is reported as retryable — against a fake, because those
// answers do not depend on a broker. This file verifies the *wire*: that a keyed produce
// really does pin a vehicle's events to one partition, and that the envelope survives
// serialization intact.
//
// It imports franz-go directly rather than going through internal/platform/broker. That is
// deliberate and confined to tests: the property under examination is the behaviour of the
// broker itself, and asking this module's own wrapper whether the wrapper worked would prove
// nothing about the partition a record actually reached.

// testTopic creates a uniquely-named topic and removes it when the test ends.
//
// It manages its own clients rather than borrowing one from the caller, and that is not
// tidiness. The first version took the caller's client and registered the delete with
// t.Cleanup — which runs *after* the test body's deferred Close, so every delete was attempted
// against a closed client, failed, and was swallowed by a t.Logf. The tell was three
// telemetry.test.* topics accumulating in the broker; the code looked correct at every line.
func testTopic(t *testing.T, seeds []string, partitions int) string {
	t.Helper()

	name := "telemetry.test." + strings.ReplaceAll(uuid.NewString(), "-", "")[:16]

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	admin, err := broker.NewClient(ctx, broker.Options{SeedBrokers: seeds, ClientID: "esp-test-admin"})
	if err != nil {
		t.Fatalf("connect to broker at %v: %v", seeds, err)
	}
	defer admin.Close()

	if _, err := broker.EnsureTopics(ctx, admin, []broker.TopicSpec{
		{Name: name, Partitions: partitions, Retention: time.Hour},
	}); err != nil {
		t.Fatalf("create test topic %s: %v", name, err)
	}

	t.Cleanup(func() {
		// A separate context *and* a separate client: the test's own context is cancelled by
		// the time cleanup runs, and any client the test body owns may already be closed.
		dctx, dcancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer dcancel()

		cleanup, err := broker.NewClient(dctx, broker.Options{SeedBrokers: seeds, ClientID: "esp-test-cleanup"})
		if err != nil {
			t.Logf("cleanup could not reach the broker to delete %s: %v", name, err)
			return
		}
		defer cleanup.Close()

		if _, err := kadm.NewClient(cleanup).DeleteTopics(dctx, name); err != nil {
			t.Logf("could not delete test topic %s: %v", name, err)
			return
		}
		t.Logf("removed test topic %s", name)
	})
	return name
}

// TestIngest_LandsOnTheBrokerWithTheVehicleAsKey is the integration test behind ADR-005.
//
// The ordering guarantee is only true if every event for one vehicle lands on one partition.
// With a fake publisher that is unprovable — the fake stores whatever key it is handed — so
// this test sends a batch through the real HTTP handler, through franz-go, onto a real
// three-partition topic, and then reads the records back and asks the *broker* which
// partition each one went to.
func TestIngest_LandsOnTheBrokerWithTheVehicleAsKey(t *testing.T) {
	seeds := testsupport.RequireBroker(t)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	const partitions = 3
	topic := testTopic(t, seeds, partitions)

	producer, err := broker.NewProducer(ctx, broker.Options{SeedBrokers: seeds, ClientID: "esp-test-producer"})
	if err != nil {
		t.Fatalf("build producer: %v", err)
	}
	defer producer.Close()

	handler, err := ingest.New(producer, ingest.Options{
		Topic:        topic,
		Source:       "integration-test",
		MaxBodyBytes: 1 << 20,
		MaxBatchSize: 100,
		Log:          discardLogger(),
	})
	if err != nil {
		t.Fatalf("ingest.New: %v", err)
	}

	mux := http.NewServeMux()
	handler.Mount(mux)
	srv := httptest.NewServer(reqid.Middleware(mux))
	defer srv.Close()

	// Four vehicles, five reports each. Twenty records across three partitions is enough for
	// a same-key-same-partition property to be meaningful: with a random partitioner the odds
	// of five records for one key coinciding are 3^-4 per vehicle.
	const vehicles, perVehicle = 4, 5
	ids := make(map[string][]string) // vehicle -> the event ids we chose, in order

	var events []json.RawMessage
	base := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	for v := 0; v < vehicles; v++ {
		vehicle := fmt.Sprintf("v-%03d", v)
		for s := 0; s < perVehicle; s++ {
			id := uuid.Must(uuid.NewV7()).String()
			ids[vehicle] = append(ids[vehicle], id)
			events = append(events, mustJSON(t, map[string]any{
				"vehicle_id": vehicle,
				"route_id":   "r-integration",
				"lat":        12.9716 + float64(v)*0.001,
				"lon":        77.5946 + float64(s)*0.001,
				"speed_kph":  32.4,
				"event_ts":   base.Add(time.Duration(s) * time.Second).Format(time.RFC3339),
				"sequence":   s + 1,
				"event_id":   id,
			}))
		}
	}

	body, err := json.Marshal(map[string]any{"events": events})
	if err != nil {
		t.Fatalf("marshal batch: %v", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/events", strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("POST /v1/events: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusAccepted {
		var errBody json.RawMessage
		_ = json.NewDecoder(resp.Body).Decode(&errBody)
		t.Fatalf("status = %d, want 202 (body=%s)", resp.StatusCode, errBody)
	}
	var submitted struct {
		Accepted int               `json:"accepted"`
		Rejected []event.Rejection `json:"rejected"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&submitted); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if submitted.Accepted != vehicles*perVehicle {
		t.Fatalf("accepted = %d, want %d (rejected: %v)", submitted.Accepted, vehicles*perVehicle, submitted.Rejected)
	}

	// Read them back from the broker. A separate client, consuming from the start, so the
	// partitions and offsets reported are the broker's own bookkeeping.
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.ClientID("esp-test-consumer"),
	)
	if err != nil {
		t.Fatalf("build consumer: %v", err)
	}
	defer consumer.Close()

	type seen struct {
		partition int32
		key       string
		eventID   string
		version   int
	}
	var got []seen
	deadline := time.Now().Add(45 * time.Second)
	for len(got) < vehicles*perVehicle && time.Now().Before(deadline) {
		fetches := consumer.PollRecords(ctx, 100)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("consume error: %v", errs)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			env, err := event.Decode(r.Value)
			if err != nil {
				t.Errorf("a record reached the broker that is not a decodable envelope: %v", err)
				return
			}
			got = append(got, seen{
				partition: r.Partition,
				key:       string(r.Key),
				eventID:   env.EventID,
				version:   env.SchemaVersion,
			})
		})
	}
	if len(got) != vehicles*perVehicle {
		t.Fatalf("read %d of %d records back within the deadline", len(got), vehicles*perVehicle)
	}

	// The load-bearing assertion: one vehicle, one partition.
	partitionOf := map[string]map[int32]int{}
	for _, s := range got {
		if partitionOf[s.key] == nil {
			partitionOf[s.key] = map[int32]int{}
		}
		partitionOf[s.key][s.partition]++
	}
	for vehicle, byPartition := range partitionOf {
		if len(byPartition) != 1 {
			t.Errorf("%s landed on %d partitions (%v): the key must pin a vehicle to one "+
				"partition, or the per-vehicle ordering guarantee in ADR-005 is not true",
				vehicle, len(byPartition), byPartition)
		}
	}
	if t.Failed() {
		return
	}

	// Every vehicle's five records are in its own single partition, and the events spread
	// across more than one partition overall — otherwise the assertion above would pass
	// trivially on a one-partition topic.
	usedPartitions := map[int32]bool{}
	for _, byPartition := range partitionOf {
		for p := range byPartition {
			usedPartitions[p] = true
		}
	}
	if len(usedPartitions) < 2 {
		t.Errorf("all %d vehicles hashed to %v; this topic has %d partitions and the test only "+
			"proves something about partition pinning when more than one is in use",
			vehicles, usedPartitions, partitions)
	}

	// The envelope survived the round trip, including the identity the producer chose.
	seenIDs := map[string]int{}
	for _, s := range got {
		if s.version != event.Version {
			t.Errorf("record %s has schema_version %d, want %d", s.eventID, s.version, event.Version)
		}
		seenIDs[s.eventID]++
	}
	for vehicle, want := range ids {
		for _, id := range want {
			if seenIDs[id] != 1 {
				t.Errorf("%s: event %s appeared %d times, want exactly 1", vehicle, id, seenIDs[id])
			}
		}
	}
}

// TestIngest_DuplicatesReachTheLog coverage note: the ingest layer deliberately does not
// deduplicate — it records what arrived and lets the consumer's transaction decide (ADR-006).
// That behaviour is asserted in handler_test.go, where it needs no broker; repeating it here
// with real records would add a minute of broker round trips to confirm what the log already
// says.
//
// What this file does assert about duplicates is the property the consumer depends on: two
// records with the same event_id can both exist in the log, and both are individually valid
// envelopes. If ingest rewrote identifiers, the consumer would have nothing to deduplicate on.

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

// discardLogger returns a logger that writes nowhere. These tests assert on what reached the
// broker and on response bodies, not on log lines.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}
