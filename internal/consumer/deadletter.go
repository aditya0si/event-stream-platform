package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/aditya0si/event-stream-platform/internal/platform/broker"
	"github.com/aditya0si/event-stream-platform/internal/sink"
)

// DLQSink records a refused event in two places, deliberately.
//
// The Postgres row is what an operator queries: it carries the error, the attempt count, and
// the origin coordinates, and it is what cmd/replay lists.
//
// The topic copy is what survives the sink being unavailable. A refused event whose row write
// fails because Postgres is unreachable is still durable in the log, so the bytes are not lost
// while the database is down — and because the consumer does not commit the offset when
// recording fails, the event also returns later.
//
// Neither copy is redundant: one is queryable, the other survives the queryable store's
// outage.
type DLQSink struct {
	pub   *broker.Producer
	store *sink.Store
	topic string
	log   *slog.Logger
}

// NewDLQSink builds the dead-letter sink.
func NewDLQSink(pub *broker.Producer, store *sink.Store, topic string, log *slog.Logger) (*DLQSink, error) {
	if pub == nil {
		return nil, errors.New("consumer: nil producer for the dead-letter topic")
	}
	if store == nil {
		return nil, errors.New("consumer: nil sink store")
	}
	if topic == "" {
		return nil, errors.New("consumer: no dead-letter topic configured")
	}
	if log == nil {
		return nil, errors.New("consumer: nil logger")
	}
	return &DLQSink{pub: pub, store: store, topic: topic, log: log}, nil
}

// DeadLetter publishes the refused event and records it.
//
// The key is preserved: one vehicle's refused events land in one DLQ partition in order, the
// same property the main topic provides, so replaying a single vehicle's failures is ordered
// rather than interleaved. That costs nothing and is the difference between a DLQ that can be
// reasoned about and one that cannot.
func (d *DLQSink) DeadLetter(ctx context.Context, dl sink.DeadLetter) error {
	if dl.Payload == nil {
		// A refusal with no bytes to preserve would become an unreadable row. It should be
		// impossible — the consumer always has the record value — so it is reported rather
		// than written as an empty entry that later looks like corruption.
		return fmt.Errorf("consumer: dead letter %s has no payload", dl.EventID)
	}

	// Publish first. If the row write then fails, the bytes are already in the log and the
	// offset stays uncommitted, so nothing is lost either way.
	if err := d.pub.Produce(ctx, d.topic, []broker.Record{{
		Key:   dl.Key,
		Value: dl.Payload,
	}}); err != nil {
		return fmt.Errorf("publish to %s: %w", d.topic, err)
	}

	if err := d.store.RecordDeadLetter(ctx, dl); err != nil {
		return fmt.Errorf("record dead letter %s: %w", dl.EventID, err)
	}

	d.log.Warn("event dead-lettered", "event_id", dl.EventID, "reason", dl.Reason,
		"topic", dl.Topic, "partition", dl.Partition, "offset", dl.Offset)
	return nil
}
