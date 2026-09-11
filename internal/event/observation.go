package event

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/aditya0si/event-stream-platform/internal/platform/idgen"
)

// ValidationError names the input field an observation failed on, so a client can attach the
// message to the input it came from rather than parsing prose.
//
// Field may be empty when the failure is not attributable to a single field (a body that is
// not JSON at all, for instance). Callers must handle that: a rejection with no field is
// still a rejection.
type ValidationError struct {
	Field  string
	Reason string
}

func (e *ValidationError) Error() string {
	if e.Field == "" {
		return e.Reason
	}
	return e.Field + ": " + e.Reason
}

// Observation is what a producer submits: one position report, plus an optional identity.
//
// It is a distinct type from VehiclePosition because the two carry different things. An
// observation is a request; a vehicle position is a fact that has been accepted and given an
// identity. Keeping them separate is what makes the next paragraph true rather than
// aspirational.
type Observation struct {
	VehiclePosition

	// EventID is optional, and its presence has meaning.
	//
	// When a producer supplies it, it is preserved verbatim. That is what makes a
	// retransmission detectable: the device sends the same report twice, both arrive with the
	// same event_id, and the sink applies it once (ADR-002, ADR-006). When a producer omits
	// it — a simple client, a curl command, a test — the platform mints one, and that event
	// has no retransmission semantics because there is nothing to retransmit.
	EventID string `json:"event_id,omitempty"`
}

// Accept parses one submitted observation, validates it, and turns it into an envelope ready
// to publish.
//
// It returns the envelope together with the position it was built from. The position is
// returned rather than re-decoded by the caller because it is the partition key: every event
// for one vehicle must land on one partition for the ordering guarantee in ADR-005 to mean
// anything, and decoding twice to recover a field the parser already had is how the two
// decodes eventually disagree.
//
// Errors are *ValidationError where a field is at fault.
func Accept(raw []byte, source string, now time.Time) (Envelope, VehiclePosition, error) {
	var obs Observation

	dec := json.NewDecoder(bytes.NewReader(raw))
	// Unknown fields are refused rather than ignored: a producer that sends `latitude`
	// instead of `lat` should be told. Silently accepting it would write a position of
	// (0, 0) — the Gulf of Guinea — for every report, and the client would see success.
	dec.DisallowUnknownFields()
	if err := dec.Decode(&obs); err != nil {
		return Envelope{}, VehiclePosition{}, &ValidationError{
			Reason: "not a valid observation: " + err.Error(),
		}
	}

	if err := obs.Validate(); err != nil {
		return Envelope{}, VehiclePosition{}, err
	}
	if err := obs.ValidateClock(now); err != nil {
		return Envelope{}, VehiclePosition{}, err
	}

	id := obs.EventID
	if id == "" {
		id = idgen.New()
	} else if _, err := uuid.Parse(id); err != nil {
		return Envelope{}, VehiclePosition{}, &ValidationError{
			Field:  "event_id",
			Reason: fmt.Sprintf("%q is not a UUID: an identity that cannot be compared for equality cannot deduplicate", id),
		}
	}

	env, err := Build(source, obs.VehiclePosition, id, now)
	if err != nil {
		// Build validates the same position again; reaching here means the two validations
		// disagree, which is a programming error rather than bad input. Surface it as such
		// instead of dressing it up as a client fault.
		var ve *ValidationError
		if errors.As(err, &ve) {
			return Envelope{}, VehiclePosition{}, ve
		}
		return Envelope{}, VehiclePosition{}, fmt.Errorf("observation accepted by validation but rejected when built: %w", err)
	}
	return env, obs.VehiclePosition, nil
}

// FieldOf extracts the field name from an error, or "" when the error does not name one.
func FieldOf(err error) string {
	var ve *ValidationError
	if errors.As(err, &ve) {
		return ve.Field
	}
	return ""
}
