// Package replay is the operator's side of the dead-letter queue: it lists what the consumer
// refused, shows one entry in full, and puts an entry back on the log it came from.
//
// # Why replay republishes rather than writing the row itself
//
// The obvious alternative — take the dead letter's payload and write its effects directly —
// would make replay a second implementation of the write path, with none of the first one's
// guarantees. Instead, replay puts the original bytes back on the original topic, preserving
// the event's identity, and the ordinary consumer processes it through the ordinary code.
//
// Three things follow from that, and all three are the reason for it:
//
//   - If the event had been partially applied before it failed, the deduplication key absorbs
//     it (ADR-002, ADR-006). A replay cannot double-apply.
//   - If the failure was transient (the database was down), the replay simply succeeds.
//   - If the event is genuinely undecodable, the replay fails again and is recorded again —
//     honestly, rather than by a bypass that pretends the data was fine.
package replay

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aditya0si/event-stream-platform/internal/platform/broker"
)

// States a dead letter can be in. These match the CHECK constraint on the column.
const (
	StateDead     = "dead"
	StateReplayed = "replayed"
)

// Entry is one row of the dead-letter queue.
type Entry struct {
	EventID       string
	Payload       []byte
	Error         string
	Reason        string
	Attempts      int
	Topic         string
	Partition     int32
	Offset        int64
	State         string
	FirstFailedAt time.Time
	LastFailedAt  time.Time
	ReplayedAt    *time.Time
}

// Filter narrows a listing.
type Filter struct {
	// State is "dead", "replayed", or ""/all.
	State string
	// Limit caps the rows returned. Zero means the store's default.
	Limit int
}

// Store reads and updates the dead-letter queue.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore builds a store.
func NewStore(pool *pgxpool.Pool) (*Store, error) {
	if pool == nil {
		return nil, errors.New("replay: nil pool")
	}
	return &Store{pool: pool}, nil
}

// DefaultListLimit bounds a listing when the caller does not choose. An unbounded query on a
// table an incident may have filled is how a debugging tool becomes a second outage.
const DefaultListLimit = 100

// NormalizeState maps a caller's state argument onto the two real values.
func NormalizeState(s string) (string, error) {
	switch s {
	case "", "all":
		return "", nil
	case StateDead, StateReplayed:
		return s, nil
	default:
		return "", fmt.Errorf("replay: state must be dead, replayed, or all — got %q", s)
	}
}

// List returns dead letters, most recently failed first.
//
// The rows are ordered by last_failed_at descending because the question an operator arrives
// with is "what just broke", not "what is the oldest thing here".
func (s *Store) List(ctx context.Context, f Filter) ([]Entry, error) {
	if s == nil || s.pool == nil {
		return nil, errors.New("replay: nil store")
	}
	state, err := NormalizeState(f.State)
	if err != nil {
		return nil, err
	}
	limit := f.Limit
	if limit <= 0 {
		limit = DefaultListLimit
	}

	// The state filter is written as a predicate rather than built by string concatenation:
	// two queries, both fully parameterised, rather than one query with SQL assembled at the
	// call site.
	const withState = `
		SELECT event_id, raw_payload, error, reason, attempts,
		       topic, partition, "offset", state,
		       first_failed_at, last_failed_at, replayed_at
		FROM dead_letters
		WHERE state = $1
		ORDER BY last_failed_at DESC
		LIMIT $2`
	const allStates = `
		SELECT event_id, raw_payload, error, reason, attempts,
		       topic, partition, "offset", state,
		       first_failed_at, last_failed_at, replayed_at
		FROM dead_letters
		ORDER BY last_failed_at DESC
		LIMIT $1`

	var (
		rows pgx.Rows
		qerr error
	)
	if state == "" {
		rows, qerr = s.pool.Query(ctx, allStates, limit)
	} else {
		rows, qerr = s.pool.Query(ctx, withState, state, limit)
	}
	if qerr != nil {
		return nil, fmt.Errorf("replay: list dead letters: %w", qerr)
	}
	defer rows.Close()

	var out []Entry
	for rows.Next() {
		var e Entry
		if err := rows.Scan(&e.EventID, &e.Payload, &e.Error, &e.Reason, &e.Attempts,
			&e.Topic, &e.Partition, &e.Offset, &e.State,
			&e.FirstFailedAt, &e.LastFailedAt, &e.ReplayedAt); err != nil {
			return nil, fmt.Errorf("replay: scan dead letter: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("replay: iterate dead letters: %w", err)
	}
	return out, nil
}

// Get returns one entry, or pgx.ErrNoRows when there is none.
func (s *Store) Get(ctx context.Context, eventID string) (Entry, error) {
	if s == nil || s.pool == nil {
		return Entry{}, errors.New("replay: nil store")
	}
	if eventID == "" {
		return Entry{}, errors.New("replay: no event id given")
	}

	var e Entry
	err := s.pool.QueryRow(ctx, `
		SELECT event_id, raw_payload, error, reason, attempts,
		       topic, partition, "offset", state,
		       first_failed_at, last_failed_at, replayed_at
		FROM dead_letters WHERE event_id = $1`, eventID,
	).Scan(&e.EventID, &e.Payload, &e.Error, &e.Reason, &e.Attempts,
		&e.Topic, &e.Partition, &e.Offset, &e.State,
		&e.FirstFailedAt, &e.LastFailedAt, &e.ReplayedAt)
	if err != nil {
		return Entry{}, err
	}
	return e, nil
}

// MarkReplayed flips a row to 'replayed', reporting whether this call was the one that did it.
//
// The `AND state = 'dead'` predicate is the concurrency control: two operators replaying the
// same event at once produce one update and one no-op, rather than two updates and two
// "successes" that would both publish. The publish still happens twice in that race — which is
// safe, because the sink deduplicates — but the *record* is unambiguous about who acted.
func (s *Store) MarkReplayed(ctx context.Context, eventID string) (bool, error) {
	if s == nil || s.pool == nil {
		return false, errors.New("replay: nil store")
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE dead_letters
		SET state = 'replayed', replayed_at = now()
		WHERE event_id = $1 AND state = 'dead'`, eventID)
	if err != nil {
		return false, fmt.Errorf("replay: mark %s replayed: %w", eventID, err)
	}
	return tag.RowsAffected() == 1, nil
}

// Publisher is the part of the broker this package needs.
type Publisher interface {
	Produce(ctx context.Context, topic string, recs []broker.Record) error
}

// Replayer puts dead letters back on the log.
type Replayer struct {
	store *Store
	pub   Publisher
}

// NewReplayer builds a replayer.
func NewReplayer(store *Store, pub Publisher) (*Replayer, error) {
	if store == nil {
		return nil, errors.New("replay: nil store")
	}
	if pub == nil {
		return nil, errors.New("replay: nil publisher")
	}
	return &Replayer{store: store, pub: pub}, nil
}

// Result reports what one replay did.
type Result struct {
	Entry Entry
	// Marked is false when the row was not in the 'dead' state, which means another operator
	// (or an earlier run) had already replayed it. The publish still happened, because the
	// alternative — checking and publishing non-atomically — has the same race with a worse
	// failure mode.
	Marked bool
}

// Replay publishes one dead letter back to the topic it came from and marks it replayed.
//
// The order is publish-then-mark, for the same reason the consumer commits offsets after its
// transaction: a crash between the two leaves the event published but still marked dead, so an
// operator replays it again — a duplicate, which the sink absorbs — rather than it being marked
// handled while never having been republished, which is a loss.
func (r *Replayer) Replay(ctx context.Context, eventID string) (Result, error) {
	if r == nil || r.store == nil || r.pub == nil {
		return Result{}, errors.New("replay: uninitialized replayer")
	}

	entry, err := r.store.Get(ctx, eventID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Result{}, fmt.Errorf("replay: no dead letter with event_id %s", eventID)
		}
		return Result{}, err
	}
	if entry.State != StateDead {
		// Refused rather than replayed: publishing again would be harmless (dedup absorbs it)
		// but it would also be pointless work, and reporting success would tell the operator
		// that their action changed something when it did not.
		return Result{Entry: entry}, fmt.Errorf(
			"replay: %s was already replayed at %s; nothing to do",
			eventID, formatTime(entry.ReplayedAt))
	}
	if len(entry.Payload) == 0 {
		return Result{Entry: entry}, fmt.Errorf(
			"replay: %s has no stored payload, so there is nothing to put back", eventID)
	}

	if err := r.pub.Produce(ctx, entry.Topic, []broker.Record{{
		Key:   entry.EventID,
		Value: entry.Payload,
		Headers: map[string]string{
			// The provenance headers are what make a replayed record distinguishable in the
			// log from one that arrived normally. Without them, an operator looking at the
			// topic during an incident cannot tell which records came from the recovery path.
			"x-replay":           "true",
			"x-origin-topic":     entry.Topic,
			"x-origin-partition": strconv.FormatInt(int64(entry.Partition), 10),
			"x-origin-offset":    strconv.FormatInt(entry.Offset, 10),
			"x-origin-reason":    entry.Reason,
			"x-origin-error":     truncate(entry.Error, 200),
		},
	}}); err != nil {
		return Result{Entry: entry}, fmt.Errorf("replay: republish %s to %s: %w",
			eventID, entry.Topic, err)
	}

	marked, err := r.store.MarkReplayed(ctx, eventID)
	if err != nil {
		// The event is back on the log but the row still says 'dead'. Reported as an error so
		// an operator knows the record disagrees with reality, and that replaying again is
		// safe rather than harmful.
		return Result{Entry: entry}, fmt.Errorf(
			"replay: %s was republished but could not be marked replayed; replaying it again is "+
				"safe because the sink deduplicates: %w", eventID, err)
	}
	return Result{Entry: entry, Marked: marked}, nil
}

// ReplayAll replays up to limit entries currently in the 'dead' state, returning what it did.
//
// A failure on one entry does not stop the run: an operator draining a queue after fixing an
// outage wants the ones that can be saved to be saved, and a single unrepairable event should
// not block the rest. The errors are returned alongside the successes, and the caller decides
// the exit code.
func (r *Replayer) ReplayAll(ctx context.Context, limit int) (results []Result, errs []error, err error) {
	if r == nil || r.store == nil {
		return nil, nil, errors.New("replay: uninitialized replayer")
	}
	entries, err := r.store.List(ctx, Filter{State: StateDead, Limit: limit})
	if err != nil {
		return nil, nil, err
	}
	for _, e := range entries {
		if ctx.Err() != nil {
			return results, errs, ctx.Err()
		}
		res, rerr := r.Replay(ctx, e.EventID)
		if rerr != nil {
			errs = append(errs, rerr)
			continue
		}
		results = append(results, res)
	}
	return results, errs, nil
}

// truncate bounds a string that goes into a record header. Headers are not free — they travel
// with the record and are copied into the log — and an error message is bounded by its first
// line's worth of meaning.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func formatTime(t *time.Time) string {
	if t == nil {
		return "unknown time"
	}
	return t.UTC().Format(time.RFC3339)
}
