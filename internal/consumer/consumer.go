// Package consumer is the consumer-group member: it reads events from the log, applies them
// through the sink, and commits offsets.
//
// # Where at-least-once comes from, mechanically
//
// Offsets are committed only after the sink's transaction has committed. That single ordering
// is the whole mechanism. A crash between the two means the batch is redelivered, so the
// system can duplicate — never lose. The duplicates are absorbed by the primary key on
// processed_events (ADR-002, ADR-006), which is why the claim this project makes is
// "effectively-once" and never "exactly-once".
//
// # Ordering, and what happens when one record fails
//
// Records are processed per partition, in offset order, and a failure stops that partition at
// the failed record: only the successful prefix is committed. Kafka commits a partition's
// offset as a single number, so committing past a failure would skip it permanently. Stopping
// preserves both properties at once — per-vehicle order (the partition key is the vehicle) and
// the failed event's existence.
package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/aditya0si/event-stream-platform/internal/event"
	"github.com/aditya0si/event-stream-platform/internal/platform/metrics"
	"github.com/aditya0si/event-stream-platform/internal/sink"
)

// DeadLetterer records a refused event. A refusal that is not recorded must not be committed,
// so this interface returning an error is load-bearing rather than decorative.
type DeadLetterer interface {
	DeadLetter(ctx context.Context, dl sink.DeadLetter) error
}

// dlqNamespace seeds the deterministic identifiers given to events that cannot be decoded.
//
// A payload that fails to parse has no envelope and therefore no event_id, but it still needs
// an identity: it must be listable, replayable, and idempotent under redelivery like every
// other dead letter. Its log position is the one thing it certainly has, so the identifier is
// derived from that — reproducibly, so the same record always yields the same id.
var dlqNamespace = uuid.MustParse("0f397628-915d-4017-804c-d7d748afb01a")

// Options configures the consumer.
type Options struct {
	Topic       string
	BatchSize   int
	MaxRetries  int
	BackoffBase time.Duration
	BackoffMax  time.Duration
	Log         *slog.Logger
}

// Consumer applies events from the log to the sink.
type Consumer struct {
	cl    *kgo.Client
	store *sink.Store
	dlq   DeadLetterer
	log   *slog.Logger
	opts  Options
}

// New builds a consumer around an already-configured group client.
func New(cl *kgo.Client, store *sink.Store, dlq DeadLetterer, opts Options) (*Consumer, error) {
	if cl == nil {
		return nil, errors.New("consumer: nil kafka client")
	}
	if store == nil {
		return nil, errors.New("consumer: nil sink store")
	}
	if dlq == nil {
		return nil, errors.New("consumer: nil dead-letterer: a refused event must not disappear")
	}
	if opts.Log == nil {
		return nil, errors.New("consumer: nil logger")
	}
	if opts.Topic == "" {
		return nil, errors.New("consumer: no topic")
	}
	if opts.BatchSize < 1 {
		opts.BatchSize = 200
	}
	if opts.BackoffBase <= 0 {
		opts.BackoffBase = 250 * time.Millisecond
	}
	if opts.BackoffMax < opts.BackoffBase {
		opts.BackoffMax = 30 * time.Second
	}
	if opts.MaxRetries == 0 {
		opts.MaxRetries = 5
	}
	return &Consumer{cl: cl, store: store, dlq: dlq, log: opts.Log, opts: opts}, nil
}

// Run polls and processes until the context is cancelled or the client is closed.
//
// A poll error does not end the loop: a consumer that exits on a transient broker problem
// turns a blip into a crash loop, and a process that restarts handles fewer events than one
// that waits.
//
// Every iteration that polls releases that poll's rebalance block before doing anything else
// with the result. See the comment at the release for why that ordering is load-bearing rather
// than cosmetic.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}

		fetches := c.cl.PollRecords(ctx, c.opts.BatchSize)
		records := fetches.Records()
		errs := fetches.Errors()
		clientClosed := fetches.IsClientClosed()

		if len(records) > 0 {
			metrics.ConsumerBatchSize.Observe(float64(len(records)))
			c.processBatch(ctx, records)
		}

		// Release the rebalance block this poll took — on every path, including the two that
		// exit.
		//
		// This is not tidiness. With BlockRebalanceOnPoll, an unreleased poll leaves the client
		// unable to complete a rebalance, including the one that leaving its group requires, so
		// Close blocks forever on a context that is never cancelled. In a deployed binary that
		// is a process which ignores SIGTERM and is killed after the grace period instead of
		// draining — the same class of defect as the background prober that keeps a server from
		// stopping.
		//
		// It was caught by a goroutine dump: the consumer tests had applied every event and
		// were sitting in cleanup, with LeaveGroupContext five minutes into a wait that could
		// never finish.
		//
		// The release happens after processBatch, never before: releasing earlier would let a
		// partition move while its batch is still being applied, which is the
		// commit-to-a-partition-you-no-longer-own case the option exists to prevent.
		c.cl.AllowRebalance()

		if clientClosed {
			return nil
		}
		// A cancelled poll during shutdown is expected, not a fault to report.
		if ctx.Err() != nil {
			return nil
		}

		for _, e := range errs {
			// An empty poll that times out is how waiting works; anything else is worth
			// reporting, so the conditions are named rather than the errors swallowed.
			if errors.Is(e.Err, context.DeadlineExceeded) || errors.Is(e.Err, context.Canceled) {
				continue
			}
			c.log.Warn("fetch error", "topic", e.Topic, "partition", e.Partition, "err", e.Err)
		}
	}
}

// processBatch handles one poll's records, committing each partition's successful prefix.
func (c *Consumer) processBatch(ctx context.Context, records []*kgo.Record) {
	byPartition := map[int32][]*kgo.Record{}
	for _, r := range records {
		byPartition[r.Partition] = append(byPartition[r.Partition], r)
	}

	// Deterministic order, so a failure's log line is reproducible.
	partitions := make([]int32, 0, len(byPartition))
	for p := range byPartition {
		partitions = append(partitions, p)
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })

	for _, p := range partitions {
		group := byPartition[p]
		sort.Slice(group, func(i, j int) bool { return group[i].Offset < group[j].Offset })

		var lastOK *kgo.Record
		for _, r := range group {
			if ctx.Err() != nil {
				break
			}
			if !c.processOne(ctx, r) {
				c.log.Warn("stopping this partition at a record that could not be applied",
					"partition", r.Partition, "offset", r.Offset)
				break
			}
			lastOK = r
		}

		// Commit only the successful prefix. CommitRecords takes the highest offset per
		// partition, so passing the last good record commits exactly that prefix and no more.
		if lastOK != nil {
			if err := c.cl.CommitRecords(ctx, lastOK); err != nil {
				// The work is done; the offset is not recorded, so this prefix will be
				// processed again and deduplicated. Logged rather than fatal: the next poll's
				// commit covers it.
				c.log.Error("committing offsets failed; this batch will be redelivered and "+
					"deduplicated by the sink",
					"partition", p, "offset", lastOK.Offset, "err", err)
			}
		}
	}
}

// processOne applies a single record, reporting whether it may be committed.
//
// True means the record is finished with: either it was applied, or it was durably refused.
// False means it must be redelivered — the only correct answer when the work could not be
// completed *and* the refusal could not be recorded, because committing then would drop the
// event entirely.
func (c *Consumer) processOne(ctx context.Context, r *kgo.Record) bool {
	started := time.Now()

	env, err := event.Decode(r.Value)
	if err != nil {
		// A payload that cannot be decoded will never decode. Retrying it would burn the
		// retry budget and block the partition behind it, so it is refused immediately — and
		// given an identity derived from its position, because it has no envelope to take one
		// from.
		return c.refuse(ctx, r, positionID(r), "decode", 0, err)
	}

	if !env.IsKnownEventType() {
		return c.refuse(ctx, r, env.EventID, "unknown_version", 0,
			fmt.Errorf("event_type %q is not implemented by this build (schema_version %d)",
				env.EventType, env.SchemaVersion))
	}

	pos, err := env.AsVehiclePosition()
	if err != nil {
		// Structurally decodable but semantically wrong: an out-of-range coordinate, a missing
		// field. Permanent for the same reason a decode failure is.
		return c.refuse(ctx, r, env.EventID, "decode", 0, err)
	}

	attempts := 0
	for {
		attempts++
		outcome, err := c.store.Apply(ctx, sink.Input{
			Envelope:  env,
			Position:  pos,
			Partition: r.Partition,
			Offset:    r.Offset,
		})
		metrics.ConsumerTxDuration.Observe(time.Since(started).Seconds())

		if err == nil {
			switch outcome {
			case sink.OutcomeApplied:
				metrics.ConsumerEventsTotal.WithLabelValues("applied").Inc()
			case sink.OutcomeDuplicate:
				// The evidence that at-least-once is being absorbed rather than tolerated.
				metrics.ConsumerEventsTotal.WithLabelValues("duplicate").Inc()
			case sink.OutcomeLate:
				// Preserved in history, refused by current state (ADR-005).
				metrics.ConsumerEventsTotal.WithLabelValues("late").Inc()
			}
			return true
		}

		if isPermanent(err) {
			return c.refuse(ctx, r, env.EventID, "decode", attempts, err)
		}

		if attempts > c.opts.MaxRetries {
			return c.refuse(ctx, r, env.EventID, "retries_exhausted", attempts, err)
		}

		backoff := c.backoff(attempts)
		metrics.ConsumerRetriesTotal.WithLabelValues("transient").Inc()
		c.log.Warn("applying an event failed; retrying",
			"event_id", env.EventID, "attempt", attempts, "retry_in", backoff.String(), "err", err)

		select {
		case <-ctx.Done():
			// Shutting down. Do not commit: the record returns after restart, and a duplicate
			// the sink absorbs is better than an event lost to a deploy.
			return false
		case <-time.After(backoff):
		}
	}
}

// refuse records a dead letter and reports whether the record may be committed.
//
// It may not be committed when recording failed. That distinction is the whole reason this
// helper returns a bool instead of logging and moving on: committing a refusal that was never
// written down destroys the event — the one outcome this system must never produce — and the
// failure would look like success in the logs.
func (c *Consumer) refuse(ctx context.Context, r *kgo.Record, eventID, reason string, attempts int, cause error) bool {
	dl := sink.DeadLetter{
		EventID:   eventID,
		Key:       string(r.Key),
		Payload:   r.Value,
		Error:     cause.Error(),
		Reason:    reason,
		Attempts:  attempts,
		Topic:     r.Topic,
		Partition: r.Partition,
		Offset:    r.Offset,
	}

	if err := c.dlq.DeadLetter(ctx, dl); err != nil {
		c.log.Error("could not record a refused event; leaving the offset uncommitted so it "+
			"is not lost", "event_id", eventID, "reason", reason,
			"partition", r.Partition, "offset", r.Offset, "err", err)
		return false
	}

	metrics.ConsumerEventsTotal.WithLabelValues("dead").Inc()
	metrics.DeadLetterTotal.WithLabelValues(reason).Inc()
	c.log.Warn("event refused", "event_id", eventID, "reason", reason,
		"partition", r.Partition, "offset", r.Offset, "err", cause)
	return true
}

// positionID derives a stable identifier from a record's log position, for events that cannot
// supply one of their own.
func positionID(r *kgo.Record) string {
	return uuid.NewSHA1(dlqNamespace,
		[]byte(fmt.Sprintf("%s/%d/%d", r.Topic, r.Partition, r.Offset))).String()
}

// backoff returns the delay before the next attempt: exponential with a ceiling, so a burst of
// failures does not become a retry storm against a database that is already struggling.
func (c *Consumer) backoff(attempt int) time.Duration {
	d := c.opts.BackoffBase
	for i := 1; i < attempt && d < c.opts.BackoffMax; i++ {
		d *= 2
	}
	if d > c.opts.BackoffMax {
		d = c.opts.BackoffMax
	}
	return d
}

// isPermanent reports whether retrying could ever succeed.
//
// The default is "transient", which is the safe direction. An unrecognised error is retried
// and the bounded retry budget then dead-letters it; guessing "permanent" would discard events
// that one more attempt would have accepted.
func isPermanent(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch {
		case strings.HasPrefix(pgErr.Code, "23"): // integrity constraint violation
			return true
		case strings.HasPrefix(pgErr.Code, "22"): // data exception: bad value for the column type
			return true
		case strings.HasPrefix(pgErr.Code, "08"): // connection exception
			return false
		case pgErr.Code == "40001": // serialization failure: transient by definition
			return false
		case pgErr.Code == "40P01": // deadlock detected
			return false
		}
	}
	return false
}
