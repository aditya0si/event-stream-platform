package broker

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Record is one message to publish.
//
// It is this package's own type rather than a *kgo.Record so that callers do not import the
// client library, and so the fields that matter here are the only ones on offer. Headers are
// absent deliberately: the only current use for record headers is replay metadata (M4), and
// adding the field now would invite a caller to set one that nothing reads.
type Record struct {
	// Key decides the partition. Records with the same key land on the same partition in
	// the order they were produced, which is the entire basis of the per-vehicle ordering
	// guarantee (ADR-005) — so the caller sets it to the entity whose order matters.
	Key string
	// Value is the serialized envelope.
	Value []byte
}

// Producer publishes records.
type Producer struct {
	cl *kgo.Client
}

// NewProducer builds a producer and verifies the broker is reachable.
func NewProducer(ctx context.Context, opts Options) (*Producer, error) {
	cl, err := NewClient(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Producer{cl: cl}, nil
}

// Produce publishes every record to one topic and waits for each acknowledgement.
//
// # What a failure means, and why the caller may retry the whole batch
//
// Kafka acknowledges per partition, so a batch can fail partially: some records are durable
// while others are not. The obvious alternatives are both wrong here. Returning the
// successfully-produced subset and dropping the rest loses events the caller believes were
// accepted. Tracking exactly which records landed would need the caller to reconcile a
// partial result against its own array, which pushes broker semantics up into the ingest
// handler.
//
// So Produce reports failure for the batch, and the caller retries the whole thing. That is
// safe *because of how the rest of the system is built*: every event carries its own
// event_id, and the consumer deduplicates on it (ADR-002, ADR-006). Retrying a batch that
// partially landed therefore produces duplicates in the log, and duplicates in the log are the
// case the pipeline is explicitly designed to absorb — never a second side effect.
//
// This is the trade the design makes everywhere: prefer a duplicate that is provably absorbed
// over a silence that is not provable at all.
func (p *Producer) Produce(ctx context.Context, topic string, recs []Record) error {
	if p == nil || p.cl == nil {
		return errors.New("broker: nil producer")
	}
	if len(recs) == 0 {
		return nil
	}
	if topic == "" {
		return errors.New("broker: no topic given")
	}

	krs := make([]*kgo.Record, 0, len(recs))
	for _, r := range recs {
		krs = append(krs, &kgo.Record{
			Topic: topic,
			Key:   []byte(r.Key),
			Value: r.Value,
		})
	}

	results := p.cl.ProduceSync(ctx, krs...)
	if err := results.FirstErr(); err != nil {
		failed := 0
		for _, r := range results {
			if r.Err != nil {
				failed++
			}
		}
		return fmt.Errorf("broker: %d of %d records were not acknowledged: %w", failed, len(recs), err)
	}
	return nil
}

// Ping checks the producer's connection, backing a readiness probe.
func (p *Producer) Ping(ctx context.Context) error {
	if p == nil || p.cl == nil {
		return errors.New("broker: nil producer")
	}
	return Ping(ctx, p.cl)
}

// Close releases the client. Safe to call on a nil or already-closed producer.
func (p *Producer) Close() {
	if p != nil && p.cl != nil {
		p.cl.Close()
	}
}
