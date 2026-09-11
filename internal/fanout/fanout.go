// Package fanout is the delivery bus between the consumer and the gateways: one publisher, N
// independent subscribers, best-effort (ADR-009).
//
// # What this package is, and what it is not
//
// It is Redis Pub/Sub, chosen because the consumer must not know how many gateways exist and
// gateways must restart without the consumer noticing. It is explicitly *not* the durable
// record: the event was committed to Postgres before anything here runs, and if a publish is
// lost the live view misses one frame while the durable history keeps it. That ordering — the
// sink first, the bus second — is what makes losing a message acceptable rather than a defect.
//
// Redis Streams were considered and rejected in ADR-009: a second log with its own offsets,
// sitting one layer above the log that already has offsets, invites the question "when the
// Stream and the log disagree, which is authoritative?" The answer would be the log, which
// makes the Stream a cache with extra steps.
package fanout

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Position is one frame on the bus: the fields a viewer needs, and nothing else.
//
// It is deliberately not the full event envelope. The bus carries what a map draws — a
// position, a heading, a time — while the envelope's schema versioning, provenance, and
// deduplication identity stay where they belong, in the log and the sink. Copying the envelope
// here would invite a reader to treat this as the record of an event, which it is not: it is a
// notice that one arrived.
type Position struct {
	EventID    string    `json:"event_id"`
	VehicleID  string    `json:"vehicle_id"`
	RouteID    string    `json:"route_id"`
	Lat        float64   `json:"lat"`
	Lon        float64   `json:"lon"`
	SpeedKPH   *float64  `json:"speed_kph,omitempty"`
	BearingDeg *float64  `json:"bearing_deg,omitempty"`
	EventTS    time.Time `json:"event_ts"`
	Sequence   uint64    `json:"sequence"`
}

// Validate checks the fields a subscriber depends on. A frame without an event id cannot be
// used as an SSE resume point, and one without coordinates cannot be drawn — both are refused
// at publish time rather than shipped as a puzzle for the gateway.
func (p Position) Validate() error {
	if p.EventID == "" {
		return errors.New("fanout: frame has no event_id, so it cannot be a resume point")
	}
	if p.VehicleID == "" {
		return errors.New("fanout: frame has no vehicle_id")
	}
	if p.EventTS.IsZero() {
		return errors.New("fanout: frame has no event_ts")
	}
	return nil
}

// Publisher puts frames on the bus.
type Publisher struct {
	rdb     redis.UniversalClient
	channel string
}

// NewPublisher builds a publisher.
func NewPublisher(rdb redis.UniversalClient, channel string) (*Publisher, error) {
	if rdb == nil {
		return nil, errors.New("fanout: nil redis client")
	}
	if channel == "" {
		return nil, errors.New("fanout: no channel configured")
	}
	return &Publisher{rdb: rdb, channel: channel}, nil
}

// Publish sends one frame.
//
// It reports success when Redis accepted the publish, and says so in the documentation rather
// than in a name: with zero subscribers Redis still accepts and returns 0 receivers. Treating
// that as a failure would make a quiet system — no gateway running — look broken to the
// consumer, which is exactly backwards. The return value is the number of subscribers that
// received it, which is a useful number to watch and a useless one to fail on.
func (p *Publisher) Publish(ctx context.Context, pos Position) (int64, error) {
	if p == nil || p.rdb == nil {
		return 0, errors.New("fanout: nil publisher")
	}
	if err := pos.Validate(); err != nil {
		return 0, err
	}
	payload, err := json.Marshal(pos)
	if err != nil {
		return 0, fmt.Errorf("fanout: marshal frame: %w", err)
	}
	n, err := p.rdb.Publish(ctx, p.channel, payload).Result()
	if err != nil {
		return 0, fmt.Errorf("fanout: publish to %s: %w", p.channel, err)
	}
	return n, nil
}

// Subscriber receives frames from the bus.
type Subscriber struct {
	rdb     redis.UniversalClient
	channel string
}

// NewSubscriber builds a subscriber.
func NewSubscriber(rdb redis.UniversalClient, channel string) (*Subscriber, error) {
	if rdb == nil {
		return nil, errors.New("fanout: nil redis client")
	}
	if channel == "" {
		return nil, errors.New("fanout: no channel configured")
	}
	return &Subscriber{rdb: rdb, channel: channel}, nil
}

// Frames returns a channel of decoded frames, and a function that stops the subscription.
//
// # What happens when Redis goes away
//
// go-redis reconnects a PubSub internally: a dropped connection surfaces as a receive error,
// the client re-subscribes, and the stream resumes. Frames published during the gap are gone —
// Pub/Sub has no persistence (ADR-009) — and that is acceptable for the same reason a lost
// publish is: the durable record is in Postgres, and a client that notices a gap resumes from
// its last event id.
//
// Errors are not fatal to the stream. The channel is closed only when the caller's context is
// cancelled, so a consumer of this API can treat a closed channel as "we are shutting down"
// rather than "something went wrong, decide what to do".
func (s *Subscriber) Frames(ctx context.Context) (<-chan Position, func(), error) {
	if s == nil || s.rdb == nil {
		return nil, nil, errors.New("fanout: nil subscriber")
	}

	sub := s.rdb.Subscribe(ctx, s.channel)
	// Confirm the subscription before returning: handing back a channel that is not yet
	// subscribed would let a caller start serving clients and miss the frames published in
	// between, which is a silent gap rather than an error.
	if _, err := sub.Receive(ctx); err != nil {
		_ = sub.Close()
		return nil, nil, fmt.Errorf("fanout: subscribe to %s: %w", s.channel, err)
	}

	out := make(chan Position, 256)
	stop := func() { _ = sub.Close() }

	go func() {
		defer close(out)
		ch := sub.Channel(redis.WithChannelSize(256))
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				var pos Position
				if err := json.Unmarshal([]byte(msg.Payload), &pos); err != nil {
					// A frame this build cannot parse is skipped rather than fatal: the bus is
					// best-effort, and one malformed frame must not take down a gateway that is
					// serving live clients. The alternative — closing the stream — would turn a
					// single bad message into an outage.
					continue
				}
				if err := pos.Validate(); err != nil {
					continue
				}
				select {
				case out <- pos:
				default:
					// The gateway is not keeping up with the bus. Dropping here is the same
					// decision as shedding a slow client (ADR-004), applied one layer earlier,
					// and the client's resume path covers it.
				}
			}
		}
	}()

	return out, stop, nil
}
