// Package sink writes accepted events to Postgres: an append-only history, a current-state
// row per vehicle, and the deduplication record that makes the whole pipeline idempotent.
//
// # The one sentence this package exists to make true
//
// A redelivered event produces no second side effect. It is not enforced by comparing
// timestamps or by a sequence check in Go — every one of those has a race — but by a PRIMARY
// KEY on processed_events, inserted inside the same transaction as the effects it guards
// (ADR-002, ADR-006). If the transaction commits, the event was applied exactly once and the
// record proving it is durable. If it rolls back, nothing happened at all: no record, no
// effects, and the event is redelivered and retried safely.
//
// # Three outcomes, all of them normal
//
//	applied    the event was new and its state was written
//	duplicate  this event_id was already committed — counted, not applied
//	late       the event was new but not newer than the vehicle's current state: recorded
//	           in the history, refused by the current-state row (ADR-005)
//
// A "late" event is not an error. An observation that arrives after a newer one is a real
// thing a fleet produces, and the system's job is to preserve it and not let it move the map
// backwards.
package sink

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aditya0si/event-stream-platform/internal/event"
	"github.com/aditya0si/event-stream-platform/internal/platform/db"
)

// Outcome is what happened to one event.
type Outcome string

const (
	OutcomeApplied   Outcome = "applied"
	OutcomeDuplicate Outcome = "duplicate"
	OutcomeLate      Outcome = "late"
)

// Input is one decoded event plus where it came from.
//
// The provenance fields are not decoration: partition and offset are written to both the
// history row and the dedup row, so any row can be traced back to the exact position in the
// log that produced it. Without them, "why does this vehicle show this position?" has no
// answer that does not involve guessing from timestamps.
type Input struct {
	Envelope  event.Envelope
	Position  event.VehiclePosition
	Partition int32
	Offset    int64
}

// Store applies events to Postgres.
type Store struct {
	pool *pgxpool.Pool
	// consumerID identifies which process applied an event. It is written to
	// processed_events so that "two consumers both applied this" is distinguishable from
	// "one consumer applied it twice" during an incident — the two have different causes.
	consumerID string
}

// NewStore builds a store. consumerID should identify the process, typically hostname+pid.
func NewStore(pool *pgxpool.Pool, consumerID string) (*Store, error) {
	if pool == nil {
		return nil, errors.New("sink: nil pool")
	}
	if consumerID == "" {
		return nil, errors.New("sink: a consumer id is required; without it a duplicate cannot be attributed")
	}
	return &Store{pool: pool, consumerID: consumerID}, nil
}

// Apply writes one event, reporting what happened.
//
// The entire effect — dedup record, history row, current state, schema-version counter — is
// one transaction. There is deliberately no way to call a piece of this separately: a caller
// that could insert the dedup record without the effect would be able to mark an event as
// handled while losing it, which is the one outcome this design must never produce.
func (s *Store) Apply(ctx context.Context, in Input) (Outcome, error) {
	if s == nil || s.pool == nil {
		return "", errors.New("sink: nil store")
	}

	var outcome Outcome
	err := db.InTx(ctx, s.pool, func(tx pgx.Tx) error {
		// 1. The deduplication gate.
		//
		// ON CONFLICT DO NOTHING makes this atomic without a separate SELECT: two concurrent
		// deliveries of the same event_id cannot both see zero rows affected. Whichever
		// transaction loses the insert race waits on the winner's lock and then reports a
		// conflict — the serialisation is Postgres's, not ours.
		tag, err := tx.Exec(ctx, `
			INSERT INTO processed_events (event_id, consumer_id, partition, "offset")
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (event_id) DO NOTHING`,
			in.Envelope.EventID, s.consumerID, in.Partition, in.Offset)
		if err != nil {
			return fmt.Errorf("insert processed_events: %w", err)
		}
		if tag.RowsAffected() == 0 {
			// Already applied by an earlier transaction. Commit (trivially) and report it.
			// Nothing else in this closure runs, so a duplicate cannot double-count the
			// schema-version counter either.
			outcome = OutcomeDuplicate
			return nil
		}

		// 2. The append-only history. The record of what a vehicle reported never changes,
		//    including when a late event arrives (step 3 refuses it, this table keeps it).
		if _, err := tx.Exec(ctx, `
			INSERT INTO vehicle_positions (
				event_id, vehicle_id, route_id, lat, lon, speed_kph, bearing_deg,
				event_ts, produced_at, partition, "offset", sequence
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
			ON CONFLICT (event_id) DO NOTHING`,
			in.Envelope.EventID, in.Position.VehicleID, in.Position.RouteID,
			in.Position.Lat, in.Position.Lon, in.Position.SpeedKPH, in.Position.BearingDeg,
			in.Position.EventTS, in.Envelope.ProducedAt, in.Partition, in.Offset,
			int64(in.Position.Sequence),
		); err != nil {
			return fmt.Errorf("insert vehicle_positions: %w", err)
		}

		// 3. The current state, guarded on event time.
		//
		// The WHERE clause is the late-data rule (ADR-005) and it lives here rather than in
		// Go so that it holds for every writer, present and future. RowsAffected 0 means the
		// incoming event is not newer than what is already recorded: the history keeps it,
		// the map does not move backwards.
		tag, err = tx.Exec(ctx, `
			INSERT INTO vehicle_current (
				vehicle_id, route_id, lat, lon, speed_kph, bearing_deg,
				last_event_ts, last_event_id
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			ON CONFLICT (vehicle_id) DO UPDATE SET
				route_id      = EXCLUDED.route_id,
				lat           = EXCLUDED.lat,
				lon           = EXCLUDED.lon,
				speed_kph     = EXCLUDED.speed_kph,
				bearing_deg   = EXCLUDED.bearing_deg,
				last_event_ts = EXCLUDED.last_event_ts,
				last_event_id = EXCLUDED.last_event_id,
				updated_at    = now()
			WHERE EXCLUDED.last_event_ts > vehicle_current.last_event_ts`,
			in.Position.VehicleID, in.Position.RouteID,
			in.Position.Lat, in.Position.Lon, in.Position.SpeedKPH, in.Position.BearingDeg,
			in.Position.EventTS, in.Envelope.EventID,
		)
		if err != nil {
			return fmt.Errorf("upsert vehicle_current: %w", err)
		}
		if tag.RowsAffected() == 0 {
			outcome = OutcomeLate
		} else {
			outcome = OutcomeApplied
		}

		// 4. Observed schema versions. Counted here rather than at ingest so the number
		//    means "versions this system has actually processed" — a producer's version that
		//    never survives the consumer is exactly the drift worth seeing.
		if _, err := tx.Exec(ctx, `
			INSERT INTO schema_versions (schema_version, event_type, event_count)
			VALUES ($1, $2, 1)
			ON CONFLICT (schema_version, event_type) DO UPDATE SET
				event_count  = schema_versions.event_count + 1,
				last_seen_at = now()`,
			in.Envelope.SchemaVersion, in.Envelope.EventType,
		); err != nil {
			return fmt.Errorf("upsert schema_versions: %w", err)
		}

		return nil
	})
	if err != nil {
		return "", err
	}
	return outcome, nil
}

// CurrentState is a vehicle's latest known position, as the API and the viewer read it.
type CurrentState struct {
	VehicleID   string
	RouteID     string
	Lat         float64
	Lon         float64
	SpeedKPH    *float64
	BearingDeg  *float64
	LastEventTS time.Time
	LastEventID string
	UpdatedAt   time.Time
}

// GetCurrent returns one vehicle's current state, or pgx.ErrNoRows when the vehicle has never
// been seen. It exists so the API can answer "where is this vehicle" without a query written
// at the call site — one place to get the column order right.
func (s *Store) GetCurrent(ctx context.Context, vehicleID string) (CurrentState, error) {
	var cs CurrentState
	err := s.pool.QueryRow(ctx, `
		SELECT vehicle_id, route_id, lat, lon, speed_kph, bearing_deg,
		       last_event_ts, last_event_id, updated_at
		FROM vehicle_current WHERE vehicle_id = $1`, vehicleID,
	).Scan(&cs.VehicleID, &cs.RouteID, &cs.Lat, &cs.Lon, &cs.SpeedKPH, &cs.BearingDeg,
		&cs.LastEventTS, &cs.LastEventID, &cs.UpdatedAt)
	if err != nil {
		return CurrentState{}, err
	}
	return cs, nil
}

// Ping checks the database, backing a readiness probe.
func (s *Store) Ping(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return errors.New("sink: nil store")
	}
	return s.pool.Ping(ctx)
}
