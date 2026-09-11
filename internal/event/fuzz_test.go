package event_test

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/aditya0si/event-stream-platform/internal/event"
)

// Fuzzing the two parsers that read untrusted bytes.
//
// DESIGN.md's M8 names "fuzz/malformed input" explicitly, and these two functions are where it
// belongs: `Accept` parses what a producer POSTs to the ingest endpoint, and `Decode` parses what
// the consumer reads back off the log. Both take bytes this system did not produce. The unit
// tests beside them cover the malformed inputs I thought of; a fuzzer covers the ones I did not.
//
// # What a failure looks like
//
// A panic fails the fuzz target by itself, which is the primary thing being hunted: an index out
// of range, a nil dereference, an unbounded allocation from a length prefix. But a parser can
// also be wrong without crashing, so the accepted path asserts the invariants that the rest of
// the system depends on — the ones a caller would never re-check:
//
//   - event_id parses as a UUID, because it is the deduplication key and an identifier that
//     cannot be compared for equality cannot deduplicate;
//   - schema_version is the version this build implements, because an unrecognised version is
//     refused rather than guessed at;
//   - the payload survives a marshal/decode round trip with the same identity, because the
//     consumer deduplicates on the id it reads back, not on the one it wrote.
//
// # Running them
//
//	go test -run=NONE -fuzz=FuzzAccept  -fuzztime=60s ./internal/event/
//	go test -run=NONE -fuzz=FuzzDecode  -fuzztime=60s ./internal/event/
//
// They are ordinary tests when run without -fuzz, which is what CI does: the seed corpus is
// executed on every run, so a regression in a known-bad input is caught by the normal suite.

// fuzzNow is fixed rather than time.Now so the fuzzer's inputs stay the only variable. The
// clock-skew check rejects an event_ts too far in the future, and a moving "now" would make that
// boundary flicker between runs.
var fuzzNow = time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)

// Seeds: one valid observation, the envelope shape that must be refused, and a few malformed
// shapes. A fuzzer with only a valid seed spends its early iterations rediscovering that junk
// parses to an error; these give it both sides of the boundary immediately.
func FuzzAccept(f *testing.F) {
	f.Add([]byte(`{"event_id":"018f3b1c-9c1e-7a2b-8d3f-4e5a6b7c8d9e","vehicle_id":"veh-1",` +
		`"route_id":"r-1","lat":12.97,"lon":77.59,"speed_kph":21.5,"bearing_deg":45.0,` +
		`"event_ts":"2026-09-11T11:59:00Z","sequence":1}`))
	f.Add([]byte(`{"vehicle_id":"veh-2","route_id":"r-1","lat":0,"lon":0,"event_ts":"2026-09-11T11:59:00Z"}`))
	// The envelope shape: refused, because the platform owns these fields. Pinned as a seed so a
	// change that starts accepting it fails a run rather than passing quietly.
	f.Add([]byte(`{"schema_version":1,"event_type":"vehicle.position","payload":{"vehicle_id":"v"}}`))
	f.Add([]byte(``))
	f.Add([]byte(`{`))
	f.Add([]byte(`[]`))
	f.Add([]byte(`{"vehicle_id":"","lat":1e400}`))
	f.Add([]byte(`{"event_id":"not-a-uuid","vehicle_id":"v","lat":90,"lon":180}`))
	f.Add([]byte(`{"vehicle_id":"v\u0000x","lat":-90,"lon":-180,"sequence":18446744073709551615}`))

	f.Fuzz(func(t *testing.T, raw []byte) {
		env, vp, err := event.Accept(raw, "fuzz", fuzzNow)
		if err != nil {
			// A refusal is a correct outcome for malformed input, and the error must be
			// non-nil-free in the sense that callers can classify it.
			if event.FieldOf(err) == "" && err.Error() == "" {
				t.Fatalf("refused with an empty error: %q", raw)
			}
			return
		}

		// Accepted. Everything below must hold, or the pipeline is carrying an event it cannot
		// deduplicate or a version it cannot interpret.
		if _, err := uuid.Parse(env.EventID); err != nil {
			t.Fatalf("accepted an event whose id is not a UUID: %q (id %q)", raw, env.EventID)
		}
		if env.SchemaVersion != event.Version {
			t.Fatalf("accepted schema_version %d, this build implements %d: %q",
				env.SchemaVersion, event.Version, raw)
		}
		if !env.IsKnownEventType() {
			t.Fatalf("accepted an unknown event type %q: %q", env.EventType, raw)
		}
		if vp.VehicleID == "" || vp.RouteID == "" {
			t.Fatalf("accepted an event with no identity: %q -> %+v", raw, vp)
		}

		// The round trip: what the consumer will read back must carry the same identity, because
		// deduplication keys on the id as it appears in the log, not the id the producer supplied.
		wire, err := env.Marshal()
		if err != nil {
			t.Fatalf("a validated envelope could not be marshalled: %v (input %q)", err, raw)
		}
		back, err := event.Decode(wire)
		if err != nil {
			t.Fatalf("an envelope this build produced could not be decoded: %v (input %q)", err, raw)
		}
		if back.EventID != env.EventID {
			t.Fatalf("round trip changed the event id: %q -> %q", env.EventID, back.EventID)
		}
		if back.SchemaVersion != env.SchemaVersion {
			t.Fatalf("round trip changed the schema version: %d -> %d",
				env.SchemaVersion, back.SchemaVersion)
		}
		// And the payload the consumer will apply must be the one that was validated.
		rt, err := back.AsVehiclePosition()
		if err != nil {
			t.Fatalf("an accepted payload failed to re-parse: %v (input %q)", err, raw)
		}
		if rt.VehicleID != vp.VehicleID {
			t.Fatalf("round trip changed the partition key: %q -> %q", vp.VehicleID, rt.VehicleID)
		}
	})
}

// FuzzDecode covers the consumer's side: bytes read straight off the log, which may have been
// written by any producer and are not guaranteed to be an envelope at all.
func FuzzDecode(f *testing.F) {
	valid, err := event.Build("fuzz", event.VehiclePosition{
		VehicleID: "veh-1", RouteID: "r-1", Lat: 12.97, Lon: 77.59,
		SpeedKPH: ptr(21.5), BearingDeg: ptr(45.0), EventTS: fuzzNow, Sequence: 1,
	}, "018f3b1c-9c1e-7a2b-8d3f-4e5a6b7c8d9e", fuzzNow)
	if err == nil {
		if wire, mErr := valid.Marshal(); mErr == nil {
			f.Add(wire)
		}
	}
	f.Add([]byte(``))
	f.Add([]byte(`null`))
	f.Add([]byte(`{"payload":null}`))
	f.Add([]byte(`{"schema_version":999,"event_id":"x","payload":{}}`))
	f.Add([]byte(`{"payload":{"lat":91}}`))
	// A payload that is valid JSON of the wrong shape: the envelope decodes, the observation
	// inside it does not.
	f.Add([]byte(`{"schema_version":1,"event_id":"018f3b1c-9c1e-7a2b-8d3f-4e5a6b7c8d9e",` +
		`"event_type":"vehicle.position","produced_at":"2026-09-11T12:00:00Z","source":"fuzz",` +
		`"payload":[1,2,3]}`))

	f.Fuzz(func(t *testing.T, raw []byte) {
		env, err := event.Decode(raw)
		if err != nil {
			return
		}
		// Decoding succeeded. The consumer's next steps are a schema check and a payload parse,
		// and neither may panic on input that decoded.
		_ = env.IsKnownEventType()
		if _, err := env.AsVehiclePosition(); err != nil {
			return
		}
		if env.EventID == "" {
			t.Fatalf("decoded an envelope with no event id: %q", raw)
		}
		// A decoded envelope must survive re-marshalling: the replay path writes an event's
		// original bytes back, and the smoke test asserts they come back unchanged.
		if _, err := env.Marshal(); err != nil {
			t.Fatalf("a decoded envelope could not be re-marshalled: %v (input %q)", err, raw)
		}
	})
}

func ptr[T any](v T) *T { return &v }
