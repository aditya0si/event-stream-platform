package event_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/aditya0si/event-stream-platform/internal/event"
)

// validPosition returns a payload that passes every check, so each test can vary exactly one
// field and be sure about which one it is testing.
func validPosition() event.VehiclePosition {
	speed := 32.4
	bearing := 187.0
	return event.VehiclePosition{
		VehicleID:  "v-042",
		RouteID:    "r-7",
		Lat:        12.9716,
		Lon:        77.5946,
		SpeedKPH:   &speed,
		BearingDeg: &bearing,
		EventTS:    time.Date(2026, 9, 11, 10, 15, 29, 900_000_000, time.UTC),
		Sequence:   1841,
	}
}

// envelopeJSON builds a raw envelope around a payload, with every header field settable so
// that header tests do not have to go through Build (which validates).
func envelopeJSON(t *testing.T, mutate func(map[string]any)) []byte {
	t.Helper()
	payload, err := json.Marshal(validPosition())
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	m := map[string]any{
		"schema_version": event.Version,
		"event_id":       uuid.Must(uuid.NewV7()).String(),
		"event_type":     event.EventTypeVehiclePosition,
		"produced_at":    "2026-09-11T10:15:30Z",
		"source":         "test",
		"payload":        json.RawMessage(payload),
	}
	if mutate != nil {
		mutate(m)
	}
	out, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return out
}

func TestDecodeAcceptsAWellFormedEnvelope(t *testing.T) {
	raw := envelopeJSON(t, nil)
	env, err := event.Decode(raw)
	if err != nil {
		t.Fatalf("Decode rejected a well-formed envelope: %v", err)
	}
	if env.SchemaVersion != event.Version {
		t.Errorf("SchemaVersion = %d, want %d", env.SchemaVersion, event.Version)
	}
	if !env.IsKnownEventType() {
		t.Errorf("IsKnownEventType = false for %q", env.EventType)
	}
	vp, err := env.AsVehiclePosition()
	if err != nil {
		t.Fatalf("AsVehiclePosition: %v", err)
	}
	if vp.VehicleID != "v-042" || vp.Sequence != 1841 {
		t.Errorf("payload round-tripped wrong: %+v", vp)
	}
	if !vp.EventTS.Equal(validPosition().EventTS) {
		t.Errorf("event_ts round-tripped as %s, want %s: timestamps must survive the wire exactly, "+
			"because they are the ordering key", vp.EventTS, validPosition().EventTS)
	}
}

// TestDecodeRejectsUnknownSchemaVersion is the test behind the versioning claim. Guessing at
// a version this build does not implement is how a pipeline writes wrong data while
// reporting success.
func TestDecodeRejectsUnknownSchemaVersion(t *testing.T) {
	for _, v := range []int{0, 2, 99} {
		raw := envelopeJSON(t, func(m map[string]any) { m["schema_version"] = v })
		_, err := event.Decode(raw)
		if err == nil {
			t.Errorf("Decode accepted schema_version %d, which this build does not implement", v)
			continue
		}
		if !strings.Contains(err.Error(), "schema_version") {
			t.Errorf("schema_version %d: the error does not name the field: %v", v, err)
		}
	}
}

func TestDecodeRejectsBadIdentifiers(t *testing.T) {
	for _, id := range []string{"", "not-a-uuid", "12345"} {
		raw := envelopeJSON(t, func(m map[string]any) { m["event_id"] = id })
		if _, err := event.Decode(raw); err == nil {
			t.Errorf("Decode accepted event_id %q: an identifier that cannot be compared for "+
				"equality cannot deduplicate", id)
		}
	}
}

func TestDecodeRejectsMissingRequiredHeaderFields(t *testing.T) {
	for _, field := range []string{"event_id", "event_type", "produced_at", "payload"} {
		field := field
		raw := envelopeJSON(t, func(m map[string]any) { delete(m, field) })
		_, err := event.Decode(raw)
		if err == nil {
			t.Errorf("Decode accepted an envelope with no %s", field)
			continue
		}
		if !strings.Contains(err.Error(), field) {
			t.Errorf("missing %s: the error does not name the field: %v", field, err)
		}
	}
}

// TestDecodeRejectsUnknownHeaderFields: a producer that spells a field differently should
// hear about it. Silently ignoring `event_timestamp` would leave event_ts at its zero value,
// which sorts as 1970 and loses every ordering comparison — a wrong result, reported as a
// success.
func TestDecodeRejectsUnknownHeaderFields(t *testing.T) {
	raw := envelopeJSON(t, func(m map[string]any) {
		m["event_timestamp"] = "2026-09-11T10:15:29Z"
	})
	_, err := event.Decode(raw)
	if err == nil {
		t.Fatal("Decode silently ignored an unknown header field")
	}
	if !strings.Contains(err.Error(), "event_timestamp") {
		t.Errorf("the error does not name the offending field: %v", err)
	}
}

func TestDecodeRejectsMalformedJSON(t *testing.T) {
	for _, raw := range []string{``, `{`, `[]`, `null`, `{"schema_version":`, `not json at all`} {
		if _, err := event.Decode([]byte(raw)); err == nil {
			t.Errorf("Decode accepted malformed JSON: %q", raw)
		}
	}
}

func TestDecodeReportsUnknownEventTypeWithoutFailing(t *testing.T) {
	// A producer shipping a new event type before every consumer is upgraded is normal, not
	// an error. The envelope must still decode, so the consumer can count it and
	// dead-letter it — which requires reading its header.
	raw := envelopeJSON(t, func(m map[string]any) { m["event_type"] = "vehicle.doors_opened" })
	env, err := event.Decode(raw)
	if err != nil {
		t.Fatalf("Decode rejected an unknown event type, which it must tolerate: %v", err)
	}
	if env.IsKnownEventType() {
		t.Error("IsKnownEventType returned true for a type this build does not implement")
	}
	if _, err := env.AsVehiclePosition(); err == nil {
		t.Error("AsVehiclePosition accepted an event whose type is not vehicle.position")
	}
}

// TestPayloadIsNotDecodedByDecode is why Envelope.Payload is raw JSON: the deduplication and
// dead-letter paths must be able to handle an event whose payload they cannot interpret.
func TestPayloadIsNotDecodedByDecode(t *testing.T) {
	raw := envelopeJSON(t, func(m map[string]any) {
		m["payload"] = json.RawMessage(`{"unknown":"shape","nested":{"deep":true}}`)
	})
	env, err := event.Decode(raw)
	if err != nil {
		t.Fatalf("Decode rejected an envelope whose payload it cannot interpret: %v", err)
	}
	if len(env.Payload) == 0 {
		t.Fatal("the payload was discarded instead of being passed through undecoded")
	}
	// And it is still reachable for a consumer that does understand it later.
	if _, err := env.AsVehiclePosition(); err == nil {
		t.Error("a payload with the wrong shape was accepted as a vehicle position")
	}
}

func TestVehiclePositionValidation(t *testing.T) {
	const now = "2026-09-11T10:16:00Z"
	base := func() event.VehiclePosition { return validPosition() }

	cases := []struct {
		name      string
		mutate    func(*event.VehiclePosition)
		wantField string
	}{
		{"no vehicle_id", func(v *event.VehiclePosition) { v.VehicleID = "" }, "vehicle_id"},
		{"no route_id", func(v *event.VehiclePosition) { v.RouteID = "" }, "route_id"},
		{"latitude too high", func(v *event.VehiclePosition) { v.Lat = 90.1 }, "lat"},
		{"latitude too low", func(v *event.VehiclePosition) { v.Lat = -90.1 }, "lat"},
		{"longitude too high", func(v *event.VehiclePosition) { v.Lon = 180.1 }, "lon"},
		{"longitude too low", func(v *event.VehiclePosition) { v.Lon = -180.1 }, "lon"},
		{"negative speed", func(v *event.VehiclePosition) { s := -1.0; v.SpeedKPH = &s }, "speed_kph"},
		{"bearing at 360", func(v *event.VehiclePosition) { b := 360.0; v.BearingDeg = &b }, "bearing_deg"},
		{"bearing negative", func(v *event.VehiclePosition) { b := -1.0; v.BearingDeg = &b }, "bearing_deg"},
		{"no event_ts", func(v *event.VehiclePosition) { v.EventTS = time.Time{} }, "event_ts"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := base()
			tc.mutate(&v)
			err := v.Validate()
			if err == nil {
				t.Fatalf("Validate accepted %s", tc.name)
			}
			if !strings.Contains(err.Error(), tc.wantField) {
				t.Errorf("the error does not name field %q, so a client cannot attach it to an "+
					"input: %v", tc.wantField, err)
			}
		})
	}
}

// TestBoundaryValuesAreAccepted guards against off-by-one range checks, which are the
// mistakes this kind of validation actually makes.
func TestBoundaryValuesAreAccepted(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*event.VehiclePosition)
	}{
		{"north pole", func(v *event.VehiclePosition) { v.Lat = 90 }},
		{"south pole", func(v *event.VehiclePosition) { v.Lat = -90 }},
		{"antimeridian east", func(v *event.VehiclePosition) { v.Lon = 180 }},
		{"antimeridian west", func(v *event.VehiclePosition) { v.Lon = -180 }},
		{"bearing zero", func(v *event.VehiclePosition) { b := 0.0; v.BearingDeg = &b }},
		{"bearing just under 360", func(v *event.VehiclePosition) { b := 359.999; v.BearingDeg = &b }},
		{"speed zero", func(v *event.VehiclePosition) { s := 0.0; v.SpeedKPH = &s }},
		{"null island", func(v *event.VehiclePosition) { v.Lat, v.Lon = 0, 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := validPosition()
			tc.mutate(&v)
			if err := v.Validate(); err != nil {
				t.Errorf("Validate rejected a legal value (%s): %v", tc.name, err)
			}
		})
	}
}

// TestFutureTimestampsAreRefused covers the one clock-dependent check, and pins the
// asymmetry that makes it correct: the future is bounded, the past is not.
func TestFutureTimestampsAreRefused(t *testing.T) {
	now := time.Date(2026, 9, 11, 10, 16, 0, 0, time.UTC)

	// Just inside the skew allowance.
	v := validPosition()
	v.EventTS = now.Add(event.MaxFutureSkew - time.Second)
	if err := v.ValidateClock(now); err != nil {
		t.Errorf("a timestamp inside the skew allowance was refused: %v", err)
	}

	// Beyond it.
	v = validPosition()
	v.EventTS = now.Add(event.MaxFutureSkew + time.Second)
	err := v.ValidateClock(now)
	if err == nil {
		t.Fatal("a timestamp beyond the future-skew allowance was accepted: it would win every " +
			"ordering comparison and freeze this vehicle's state")
	}
	if !strings.Contains(err.Error(), "event_ts") {
		t.Errorf("the error does not name the field: %v", err)
	}

	// Long in the past: legal. This is a late event, handled deliberately (ADR-005).
	v = validPosition()
	v.EventTS = now.Add(-72 * time.Hour)
	if err := v.ValidateClock(now); err != nil {
		t.Errorf("a late event was refused by the clock check, but late events are a supported "+
			"case rather than an error: %v", err)
	}
}

func TestBuildSetsTheVersionAndPreservesTheIdentifier(t *testing.T) {
	id := uuid.Must(uuid.NewV7()).String()
	produced := time.Date(2026, 9, 11, 10, 15, 30, 0, time.UTC)

	env, err := event.Build("simulate", validPosition(), id, produced)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if env.SchemaVersion != event.Version {
		t.Errorf("SchemaVersion = %d, want %d", env.SchemaVersion, event.Version)
	}
	if env.EventID != id {
		t.Errorf("EventID = %q, want %q: the caller's identifier must survive, because it is "+
			"what a retransmission is recognised by", env.EventID, id)
	}
	if !env.ProducedAt.Equal(produced) {
		t.Errorf("ProducedAt = %s, want %s", env.ProducedAt, produced)
	}

	// And the result is decodable, which is the only property that matters across a process
	// boundary.
	raw, err := env.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	back, err := event.Decode(raw)
	if err != nil {
		t.Fatalf("Decode(Marshal(x)) failed: %v", err)
	}
	if back.EventID != id {
		t.Errorf("round trip changed the identifier: %q -> %q", id, back.EventID)
	}
}

func TestBuildRejectsWhatItCannotSerialize(t *testing.T) {
	produced := time.Date(2026, 9, 11, 10, 15, 30, 0, time.UTC)

	if _, err := event.Build("simulate", validPosition(), "", produced); err == nil {
		t.Error("Build minted an envelope with no event_id")
	}
	if _, err := event.Build("simulate", validPosition(), "not-a-uuid", produced); err == nil {
		t.Error("Build accepted a non-UUID event_id")
	}

	bad := validPosition()
	bad.Lat = 91
	if _, err := event.Build("simulate", bad, uuid.Must(uuid.NewV7()).String(), produced); err == nil {
		t.Error("Build accepted an out-of-range position")
	}
}

// TestEventIDAndEventTSAreIndependent is the design decision from DESIGN.md § D as a test:
// a retransmission keeps both its identifier and its observation time, which is what makes
// it detectable as a duplicate rather than a new observation.
func TestEventIDAndEventTSAreIndependent(t *testing.T) {
	original := validPosition()
	id := uuid.Must(uuid.NewV7()).String()
	produced := time.Date(2026, 9, 11, 10, 15, 30, 0, time.UTC)

	first, err := event.Build("simulate", original, id, produced)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	// The same report retransmitted: same identifier, same observation time, later
	// production time.
	retransmit, err := event.Build("simulate", original, id, produced.Add(time.Minute))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	if first.EventID != retransmit.EventID {
		t.Fatal("a retransmission must keep its original event_id: that identifier is the only " +
			"thing that lets the sink recognise it as a duplicate")
	}

	a, err := first.AsVehiclePosition()
	if err != nil {
		t.Fatalf("AsVehiclePosition: %v", err)
	}
	b, err := retransmit.AsVehiclePosition()
	if err != nil {
		t.Fatalf("AsVehiclePosition: %v", err)
	}
	if !a.EventTS.Equal(b.EventTS) {
		t.Error("a retransmission must keep its original event_ts: the observation did not " +
			"happen twice, and moving its timestamp would make it look like a new one")
	}
}
