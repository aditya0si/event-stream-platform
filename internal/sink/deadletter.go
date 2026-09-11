package sink

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// DeadLetter is a refused event, with enough context to debug it without opening the broker.
//
// Two copies of a refused event are written: this row, and a message on the dead-letter topic.
// The row is what an operator queries and what cmd/replay lists; the topic copy is what
// survives this database being unreachable, which is why the consumer publishes before it
// records.
type DeadLetter struct {
	EventID string
	// Key is the original record's key — the vehicle for telemetry. Preserving it keeps one
	// vehicle's failures in one dead-letter partition and in order.
	Key string
	// Payload is stored verbatim, as bytes. It is often not valid JSON, because a payload
	// that failed to parse is one of the things this table is for (see migration 0002).
	Payload []byte
	// Error is the failure text; Reason is its category, so a query can group by cause
	// without pattern-matching messages.
	Error     string
	Reason    string
	Attempts  int
	Topic     string
	Partition int32
	Offset    int64
	FirstSeen time.Time
}

// RecordDeadLetter writes the dead-letter row.
//
// It is idempotent on event_id: a redelivered poison event updates the existing row's error
// and attempt count rather than raising a unique violation. The alternative is worse than it
// sounds — a violation would fail the insert, the caller would treat the refusal as unrecorded
// and refuse to commit, and the same poison event would be redelivered forever while blocking
// its partition.
func (s *Store) RecordDeadLetter(ctx context.Context, dl DeadLetter) error {
	if s == nil || s.pool == nil {
		return errors.New("sink: nil store")
	}
	if dl.EventID == "" {
		// A dead letter with no identifier cannot be listed, replayed, or looked up. The
		// consumer derives one from the record's position when the payload has no envelope,
		// so an empty id here means a caller skipped that step.
		return errors.New("sink: dead letter has no event id")
	}
	if dl.Reason == "" {
		dl.Reason = "unknown"
	}
	first := dl.FirstSeen
	if first.IsZero() {
		first = time.Now().UTC()
	}

	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO dead_letters (
				event_id, raw_payload, error, reason, attempts,
				first_failed_at, last_failed_at, topic, partition, "offset", state
			) VALUES ($1, $2, $3, $4, $5, $6, now(), $7, $8, $9, 'dead')
			ON CONFLICT (event_id) DO UPDATE SET
				error          = EXCLUDED.error,
				reason         = EXCLUDED.reason,
				attempts       = dead_letters.attempts + EXCLUDED.attempts,
				last_failed_at = now()`,
			dl.EventID, dl.Payload, dl.Error, dl.Reason, dl.Attempts,
			first, dl.Topic, dl.Partition, dl.Offset)
		if err != nil {
			return fmt.Errorf("record dead letter %s: %w", dl.EventID, err)
		}
		return nil
	})
}

// DLQDepth counts rows still awaiting replay.
//
// It returns a value rather than leaving the caller to query, because the number must be
// publishable as a gauge even when it is zero: a gauge that is absent when the queue is empty
// vanishes precisely when the system is healthy. The sibling repository records that lesson in
// its own ADR after shipping exactly that mistake.
func (s *Store) DLQDepth(ctx context.Context) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, errors.New("sink: nil store")
	}
	var n int64
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM dead_letters WHERE state = 'dead'`).Scan(&n); err != nil {
		return 0, fmt.Errorf("count dead letters: %w", err)
	}
	return n, nil
}
