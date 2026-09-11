// Package event defines the wire contract: the versioned envelope every message carries and
// the typed payloads it can hold.
//
// The package is deliberately the only place that knows how an event is shaped. Producers
// build envelopes through it, the consumer decodes them through it, and the replay CLI
// parses them through it — so a change to the contract cannot be applied in one place and
// forgotten in another.
package event

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	// Version is the envelope schema version this build understands and produces.
	Version = 1

	// EventTypeVehiclePosition is the only event type at v1.
	EventTypeVehiclePosition = "vehicle.position"

	// MaxFutureSkew bounds how far ahead of our clock an observation may claim to be.
	//
	// Only the future is bounded. An event from the past is not an error — it is a late
	// event, and late events are a case this system handles deliberately (ADR-005):
	// preserved in history, refused by current state. A timestamp far in the *future* is a
	// different problem, because it would win every ordering comparison and freeze that
	// vehicle's state until real time caught up. So it is refused at the boundary, where
	// the cost is one rejected event rather than a stuck vehicle.
	MaxFutureSkew = 5 * time.Minute

	// MaxBatchSize caps how many events one ingest request may carry. Enforced here as well
	// as in the HTTP layer so that any future producer path (a replay CLI, a test helper)
	// inherits the same limit rather than having to remember it.
	MaxBatchSize = 500
)

// Envelope is the versioned header every event carries, with its payload left undecoded.
//
// Payload is raw JSON rather than a typed field on purpose. The envelope's shape is stable
// across event types while payloads differ per type, and — more importantly — a consumer
// that meets an event type it does not understand must still be able to record that it saw
// one, count it, and dead-letter it. A typed payload would force every reader to understand
// every type, which is exactly the coupling schema versioning exists to avoid.
type Envelope struct {
	SchemaVersion int             `json:"schema_version"`
	EventID       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	ProducedAt    time.Time       `json:"produced_at"`
	Source        string          `json:"source"`
	Payload       json.RawMessage `json:"payload"`
}

// VehiclePosition is the payload for EventTypeVehiclePosition: one observation of one
// vehicle at one moment.
type VehiclePosition struct {
	VehicleID  string    `json:"vehicle_id"`
	RouteID    string    `json:"route_id"`
	Lat        float64   `json:"lat"`
	Lon        float64   `json:"lon"`
	SpeedKPH   *float64  `json:"speed_kph,omitempty"`
	BearingDeg *float64  `json:"bearing_deg,omitempty"`
	EventTS    time.Time `json:"event_ts"`
	// Sequence is the vehicle's own monotonic counter for its reports. It is not the
	// ordering key — event_ts is (ADR-005) — but a gap in it is how a missing report is
	// detected, which a timestamp cannot tell you.
	Sequence uint64 `json:"sequence"`
}

// Rejection names one event a batch could not accept, and why.
//
// The index refers to the position in the submitted batch, not to any identifier: a
// rejected event may be too malformed to have an id, so the only reliable way to point at
// it is where it appeared.
type Rejection struct {
	Index  int    `json:"index"`
	Field  string `json:"field,omitempty"`
	Reason string `json:"reason"`
}

// Batch is what an ingest request carries.
type Batch struct {
	Events []json.RawMessage `json:"events"`
}

// Decode parses an envelope from raw bytes and validates its header.
//
// It validates the header only. The payload is returned undecoded so that a caller which
// does not understand a given event_type — the deduplication path, the schema-version
// counter, the dead-letter writer — can still handle the event correctly.
func Decode(raw []byte) (Envelope, error) {
	var env Envelope
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	// Reject unknown fields rather than ignoring them: a producer that sends
	// `event_timestamp` instead of `event_ts` should be told, not silently accepted with a
	// zero time that then sorts as 1970 and loses every ordering comparison.
	dec.DisallowUnknownFields()
	if err := dec.Decode(&env); err != nil {
		return Envelope{}, fmt.Errorf("envelope: malformed JSON: %w", err)
	}

	if err := env.validateHeader(); err != nil {
		return Envelope{}, err
	}
	return env, nil
}

// validateHeader checks the parts of the envelope that are independent of event type.
func (e Envelope) validateHeader() error {
	if e.SchemaVersion == 0 {
		return &ValidationError{Field: "schema_version", Reason: "is required"}
	}
	if e.SchemaVersion != Version {
		// Not "guess at it": a major version this build does not implement may change the
		// meaning of fields it already knows, and interpreting it optimistically is how a
		// pipeline silently writes wrong data. The caller dead-letters it instead.
		return &ValidationError{
			Field:  "schema_version",
			Reason: fmt.Sprintf("%d is not supported by this build, which implements %d", e.SchemaVersion, Version),
		}
	}
	if e.EventID == "" {
		return &ValidationError{Field: "event_id", Reason: "is required"}
	}
	if _, err := uuid.Parse(e.EventID); err != nil {
		return &ValidationError{
			Field:  "event_id",
			Reason: fmt.Sprintf("%q is not a UUID: an identifier that cannot be compared for equality cannot deduplicate", e.EventID),
		}
	}
	if e.EventType == "" {
		return &ValidationError{Field: "event_type", Reason: "is required"}
	}
	if e.ProducedAt.IsZero() {
		return &ValidationError{Field: "produced_at", Reason: "is required"}
	}
	if len(e.Payload) == 0 {
		return &ValidationError{Field: "payload", Reason: "is required"}
	}
	return nil
}

// IsKnownMentions reports whether this build understands the envelope's event type.
//
// Unknown types are not an error — they are the normal consequence of a producer shipping
// a new type before every consumer is upgraded — but they are not silently accepted either.
func (e Envelope) IsKnownEventType() bool {
	switch e.EventType {
	case EventTypeVehiclePosition:
		return true
	default:
		return false
	}
}

// AsVehiclePosition decodes the payload as a vehicle position and validates it.
func (e Envelope) AsVehiclePosition() (VehiclePosition, error) {
	if e.EventType != EventTypeVehiclePosition {
		return VehiclePosition{}, fmt.Errorf("payload: event_type is %q, not %q",
			e.EventType, EventTypeVehiclePosition)
	}
	var vp VehiclePosition
	dec := json.NewDecoder(strings.NewReader(string(e.Payload)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&vp); err != nil {
		return VehiclePosition{}, fmt.Errorf("payload: malformed vehicle position: %w", err)
	}
	if err := vp.Validate(); err != nil {
		return VehiclePosition{}, err
	}
	return vp, nil
}

// Validate checks a vehicle position for the things that are wrong regardless of clock:
// missing identifiers, out-of-range coordinates, impossible speeds.
//
// Range checks live here rather than only in the database so that a bad event is refused at
// the boundary, with a message naming the field, instead of becoming a constraint violation
// on a consumer's transaction — where it would look like an infrastructure fault rather
// than a bad observation. The database constraints stay as the backstop (0001_init.sql).
func (v VehiclePosition) Validate() error {
	if v.VehicleID == "" {
		return &ValidationError{Field: "vehicle_id", Reason: "is required"}
	}
	if v.RouteID == "" {
		return &ValidationError{Field: "route_id", Reason: "is required"}
	}
	if v.Lat < -90 || v.Lat > 90 {
		return &ValidationError{Field: "lat", Reason: fmt.Sprintf("%v is outside [-90, 90]", v.Lat)}
	}
	if v.Lon < -180 || v.Lon > 180 {
		return &ValidationError{Field: "lon", Reason: fmt.Sprintf("%v is outside [-180, 180]", v.Lon)}
	}
	// A position of exactly (0,0) is the Gulf of Guinea, and it is also what a device
	// reports when its fix has not arrived yet. Refusing it would be wrong for a vessel
	// genuinely there, so it is accepted — but the check is worth a comment, because it
	// looks like an obvious validation to add and is not.
	if v.SpeedKPH != nil && *v.SpeedKPH < 0 {
		return &ValidationError{Field: "speed_kph", Reason: fmt.Sprintf("%v is negative", *v.SpeedKPH)}
	}
	if v.BearingDeg != nil && (*v.BearingDeg < 0 || *v.BearingDeg >= 360) {
		return &ValidationError{Field: "bearing_deg", Reason: fmt.Sprintf("%v is outside [0, 360)", *v.BearingDeg)}
	}
	if v.EventTS.IsZero() {
		return &ValidationError{Field: "event_ts", Reason: "is required"}
	}
	return nil
}

// ValidateClock checks the timestamp against now. It is separate from Validate because it is
// the only check that depends on the current time, which makes it the only one a test has to
// control.
func (v VehiclePosition) ValidateClock(now time.Time) error {
	if v.EventTS.After(now.Add(MaxFutureSkew)) {
		return &ValidationError{
			Field: "event_ts",
			Reason: fmt.Sprintf("%s is more than %s in the future (now %s): a timestamp ahead of our "+
				"clock would win every ordering comparison and freeze this vehicle's state",
				v.EventTS.UTC().Format(time.RFC3339), MaxFutureSkew, now.UTC().Format(time.RFC3339)),
		}
	}
	return nil
}

// Build assembles an envelope for a vehicle position.
//
// event_id is generated here, once, at the source. That is what makes the whole pipeline
// idempotent: a retransmitted report carries the same identifier and is therefore
// recognisable as a duplicate (ADR-002, ADR-006), while a re-sent *batch* does not become
// new observations.
func Build(source string, vp VehiclePosition, eventID string, producedAt time.Time) (Envelope, error) {
	if eventID == "" {
		return Envelope{}, errors.New("envelope: event_id is required to build an event")
	}
	if _, err := uuid.Parse(eventID); err != nil {
		return Envelope{}, fmt.Errorf("envelope: event_id %q is not a UUID: %w", eventID, err)
	}
	if err := vp.Validate(); err != nil {
		return Envelope{}, err
	}

	payload, err := json.Marshal(vp)
	if err != nil {
		return Envelope{}, fmt.Errorf("envelope: marshal payload: %w", err)
	}
	return Envelope{
		SchemaVersion: Version,
		EventID:       eventID,
		EventType:     EventTypeVehiclePosition,
		ProducedAt:    producedAt.UTC(),
		Source:        source,
		Payload:       payload,
	}, nil
}

// Marshal renders the envelope as JSON for the wire.
//
// It is separate from Build so that a caller can inspect or validate an envelope without
// committing to a serialization, and so the one place that decides the byte format is
// named.
func (e Envelope) Marshal() ([]byte, error) {
	out, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("envelope: marshal: %w", err)
	}
	return out, nil
}
