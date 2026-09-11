package sink

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/aditya0si/event-stream-platform/internal/fanout"
)

// This file holds the two reads that back the gateway's resume path (ADR-003). They live here,
// next to Apply, because they read the same table Apply writes and must agree with it about
// column order and types. The gateway depends on an interface, not on this package.

// PositionTime resolves an event id to the event time it recorded.
//
// It is the first half of a resume: a browser reconnects with `Last-Event-ID: <event id>`, and
// this turns that identity into the ordering key the history is indexed on. The boolean reports
// whether the event exists at all — a client can present an id that has aged out of retention,
// and that is a different situation from "the database is unreachable", so the two are not
// collapsed into one error.
func (s *Store) PositionTime(ctx context.Context, eventID string) (time.Time, bool, error) {
	if s == nil || s.pool == nil {
		return time.Time{}, false, errors.New("sink: nil store")
	}
	if eventID == "" {
		return time.Time{}, false, errors.New("sink: no event id")
	}

	var ts time.Time
	err := s.pool.QueryRow(ctx,
		`SELECT event_ts FROM vehicle_positions WHERE event_id = $1`, eventID).Scan(&ts)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("sink: resolve resume point %s: %w", eventID, err)
	}
	return ts, true, nil
}

// CurrentPosition returns where a vehicle is right now, as a fan-out frame.
//
// It exists because the gateway reads current state through the same interface it reads resumes
// through (gateway.CurrentReader), and without it `GET /v1/vehicles/{id}` would fail its type
// assertion and answer 500 for every request — a route that exists, is documented, and cannot
// work. The conversion lives here rather than in the gateway so the two views of "current"
// cannot drift apart.
//
// The secondary lookup is deliberate: vehicle_current records the last event id but not its
// sequence, and the frame shape carries a sequence so a viewer can spot a gap. Joining the
// history on that id is one indexed read and avoids denormalising the counter into a table whose
// whole job is the newest row.
func (s *Store) CurrentPosition(ctx context.Context, vehicleID string) (fanout.Position, bool, error) {
	if s == nil || s.pool == nil {
		return fanout.Position{}, false, errors.New("sink: nil store")
	}
	if vehicleID == "" {
		return fanout.Position{}, false, errors.New("sink: no vehicle id")
	}

	var p fanout.Position
	var seq int64
	err := s.pool.QueryRow(ctx, `
		SELECT c.vehicle_id, c.route_id, c.lat, c.lon, c.speed_kph, c.bearing_deg,
		       c.last_event_ts, c.last_event_id, coalesce(h.sequence, 0)
		FROM vehicle_current c
		LEFT JOIN vehicle_positions h ON h.event_id = c.last_event_id
		WHERE c.vehicle_id = $1`, vehicleID,
	).Scan(&p.VehicleID, &p.RouteID, &p.Lat, &p.Lon, &p.SpeedKPH, &p.BearingDeg,
		&p.EventTS, &p.EventID, &seq)
	if errors.Is(err, pgx.ErrNoRows) {
		// Not an error: a vehicle nobody has reported on yet. The boolean carries that
		// distinction so the caller can answer 404 rather than 500.
		return fanout.Position{}, false, nil
	}
	if err != nil {
		return fanout.Position{}, false, fmt.Errorf("sink: read current position for %s: %w", vehicleID, err)
	}
	if seq < 0 {
		seq = 0
	}
	p.Sequence = uint64(seq)
	return p, true, nil
}

// PositionsSince returns recorded positions strictly after a resume point, oldest first, bounded
// by limit.
//
// # Why the resume point is a pair and not a timestamp
//
// Telemetry shares seconds constantly, so `event_ts > after` silently drops every event that
// shares the resume second and has not been delivered yet — a hole in the exact guarantee this
// read exists to provide, and one nothing downstream would notice. Comparing the
// (event_ts, event_id) tuple is the same total order the frames are delivered in, it is exact
// rather than approximate, and it uses the (event_ts, event_id) index rather than defeating it.
//
// Ordering is by (event_ts, event_id) rather than by insertion, because a resumed client needs
// the events in the order they happened; `ingested_at` would put a late-arriving report at the
// wrong place in its vehicle's history. The id is the tiebreak, so a caller that stops at the
// limit has a defined prefix rather than an arbitrary subset of the same second.
//
// The limit is not optional in spirit: an unbounded replay is a denial-of-service vector
// dressed as a convenience, which is why the gateway also refuses to resume from further back
// than its configured window.
func (s *Store) PositionsSince(ctx context.Context, after time.Time, afterID string, limit int) ([]fanout.Position, error) {
	if s == nil || s.pool == nil {
		return nil, errors.New("sink: nil store")
	}
	if afterID == "" {
		return nil, errors.New("sink: a resume point requires the id of the last event the client " +
			"saw, not only its timestamp — a time is not a position in the delivery order")
	}
	if limit <= 0 {
		return nil, errors.New("sink: a positive limit is required; an unbounded replay is a DoS vector")
	}

	rows, err := s.pool.Query(ctx, `
		SELECT event_id, vehicle_id, route_id, lat, lon, speed_kph, bearing_deg,
		       event_ts, sequence
		FROM vehicle_positions
		WHERE (event_ts, event_id) > ($1, $2)
		ORDER BY event_ts, event_id
		LIMIT $3`, after, afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("sink: replay from %s: %w", after.UTC().Format(time.RFC3339), err)
	}
	defer rows.Close()

	out := make([]fanout.Position, 0, 64)
	for rows.Next() {
		var p fanout.Position
		var seq int64
		if err := rows.Scan(&p.EventID, &p.VehicleID, &p.RouteID, &p.Lat, &p.Lon,
			&p.SpeedKPH, &p.BearingDeg, &p.EventTS, &seq); err != nil {
			return nil, fmt.Errorf("sink: scan replayed position: %w", err)
		}
		// A negative sequence cannot exist — the consumer validates it — and converting a
		// negative int64 to uint64 would silently produce an enormous value rather than an
		// error, which is the kind of bug that shows up as a bizarre gap in a viewer.
		if seq < 0 {
			seq = 0
		}
		p.Sequence = uint64(seq)
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sink: iterate replayed positions: %w", err)
	}
	return out, nil
}
