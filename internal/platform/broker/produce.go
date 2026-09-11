package broker

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Record is one message to publish.
//
// It is this package's own type rather than a *kgo.Record so that callers do not import the
// client library, and so the fields that matter here are the only ones on offer.
type Record struct {
	// Key decides the partition. Records with the same key land on the same partition in
	// the order they were produced, which is the entire basis of the per-vehicle ordering
	// guarantee (ADR-005) — so the caller sets it to the entity whose order matters.
	Key string
	// Value is the serialized envelope.
	Value []byte
	// Headers travel beside the payload. Kafka does not interpret them: they exist for
	// producers and consumers to annotate a record, and the one current use is replay
	// provenance (cmd/replay marks what it put back and where the record came from).
	//
	// A map rather than a slice because a caller sets named facts, not an ordered list, and
	// duplicate keys would be a bug rather than a feature. The conversion to the wire
	// representation sorts by key so that a produced record is byte-identical across runs,
	// which matters when a test compares them.
	Headers map[string]string
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
		kr := &kgo.Record{
			Topic: topic,
			Key:   []byte(r.Key),
			Value: r.Value,
		}
		if len(r.Headers) > 0 {
			// Sorted, so the same logical record produces the same bytes on every call.
			keys := make([]string, 0, len(r.Headers))
			for k := range r.Headers {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			kr.Headers = make([]kgo.RecordHeader, 0, len(keys))
			for _, k := range keys {
				kr.Headers = append(kr.Headers, kgo.RecordHeader{Key: k, Value: []byte(r.Headers[k])})
			}
		}
		krs = append(krs, kr)
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
