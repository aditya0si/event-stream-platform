package simulator_test

import (
	"strings"
	"testing"

	"github.com/aditya0si/event-stream-platform/internal/simulator"
)

// TestLoadDefaultGeometryIsUsable is the check that would have caught the first version of this
// package: the embedded file sat in the wrong directory, so the embed pattern matched nothing
// and the package did not compile at all. A build error is loud, which is why that was fine —
// but the geometry's *content* has to be validated too, and a zero-length or unclosed route is
// only visible once something walks it.
func TestLoadDefaultGeometryIsUsable(t *testing.T) {
	g, err := simulator.LoadDefault()
	if err != nil {
		t.Fatalf("LoadDefault: %v", err)
	}
	if len(g.Routes) < 4 {
		t.Errorf("geometry has %d routes; the fleet spreads across them, so a small set makes "+
			"every vehicle a neighbour", len(g.Routes))
	}

	for _, r := range g.Routes {
		if r.Length() <= 0 {
			t.Errorf("route %s has length %v", r.ID, r.Length())
		}
		// A closed route repeats its first point, and that is what stops a vehicle teleporting
		// when it reaches the end. The distance from the last point back to the first is the
		// check: if it is large, the "loop" is a jump.
		first, last := r.Points[0], r.Points[len(r.Points)-1]
		if gap := simulator.Haversine(first, last); gap > 1 {
			t.Errorf("route %s is not closed: %v ends %.0f m from where it starts, so a vehicle "+
				"would jump across the map at the wrap", r.ID, r.Points[len(r.Points)-1], gap)
		}
	}
}

func TestAtInterpolatesAndWraps(t *testing.T) {
	g, err := simulator.LoadDefault()
	if err != nil {
		t.Fatalf("LoadDefault: %v", err)
	}
	r := &g.Routes[0]

	start, _ := r.At(0)
	if gap := simulator.Haversine(start, r.Points[0]); gap > 1 {
		t.Errorf("distance 0 along the route is %.0f m from the first point", gap)
	}

	// Halfway round must be a different place, and it must be on the loop rather than past it.
	half, _ := r.At(r.Length() / 2)
	if gap := simulator.Haversine(half, r.Points[0]); gap < 100 {
		t.Errorf("the midpoint of a %0.f m route is only %.0f m from its start; the route may "+
			"double back on itself", r.Length(), gap)
	}

	// Wrapping: one length beyond the end is the start again. This is the property that makes a
	// long-running simulation stable — without it a vehicle would leave the map.
	wrapped, _ := r.At(r.Length() * 3.5)
	halfWrapped, _ := r.At(r.Length() / 2)
	if gap := simulator.Haversine(wrapped, halfWrapped); gap > 1 {
		t.Errorf("3.5 laps and 0.5 laps are %.0f m apart; the wrap is wrong", gap)
	}

	// And a negative offset, which a caller should not produce but must not panic on.
	back, _ := r.At(-r.Length() / 2)
	if gap := simulator.Haversine(back, halfWrapped); gap > 1 {
		t.Errorf("-0.5 laps and 0.5 laps are %.0f m apart; a negative offset is mishandled", gap)
	}
}

// TestSameSeedProducesTheSameFleet is the determinism the whole benchmark rests on: if two runs
// with one seed differ, a throughput difference between them is not attributable to the system.
func TestSameSeedProducesTheSameFleet(t *testing.T) {
	g, err := simulator.LoadDefault()
	if err != nil {
		t.Fatalf("LoadDefault: %v", err)
	}

	run := func(seed int64) []simulator.Observation {
		f, err := simulator.NewFleet(g, simulator.FleetConfig{Seed: seed, Vehicles: 20})
		if err != nil {
			t.Fatalf("NewFleet: %v", err)
		}
		var out []simulator.Observation
		for i := 0; i < 10; i++ {
			out = append(out, f.Step(0.1)...)
		}
		return out
	}

	a, b := run(42), run(42)
	if len(a) != len(b) {
		t.Fatalf("same seed produced %d and %d observations", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("observation %d differs between runs with one seed:\n  %+v\n  %+v", i, a[i], b[i])
		}
	}

	// A different seed must produce a different fleet, or the seed is not doing anything and
	// the determinism above is vacuous.
	c := run(43)
	if len(c) != len(a) {
		t.Fatalf("different seeds produced %d and %d observations", len(a), len(c))
	}
	same := true
	for i := range a {
		if a[i] != c[i] {
			same = false
			break
		}
	}
	if same {
		t.Error("seeds 42 and 43 produced an identical fleet, so the seed is ignored")
	}
}

func TestStepAdvancesVehiclesAlongTheirRoutes(t *testing.T) {
	g, err := simulator.LoadDefault()
	if err != nil {
		t.Fatalf("LoadDefault: %v", err)
	}
	f, err := simulator.NewFleet(g, simulator.FleetConfig{Seed: 7, Vehicles: 12})
	if err != nil {
		t.Fatalf("NewFleet: %v", err)
	}

	routeOf := map[string]string{}
	last := map[string]simulator.Observation{}
	for _, v := range f.Vehicles() {
		routeOf[v.ID] = v.Route.ID
	}

	moved := 0
	for step := 0; step < 60; step++ {
		for _, o := range f.Step(1.0) {
			if o.VehicleID == "" || o.RouteID == "" {
				t.Fatalf("observation is missing an identity: %+v", o)
			}
			if o.RouteID != routeOf[o.VehicleID] {
				t.Fatalf("vehicle %s reported route %s, was placed on %s",
					o.VehicleID, o.RouteID, routeOf[o.VehicleID])
			}
			// The band the fleet is placed in, with room for the speed oscillation. A value
			// outside it would mean the modulation has a bug that would show up in the viewer
			// as vehicles either parked or supersonic.
			if o.SpeedKPH < 10 || o.SpeedKPH > 110 {
				t.Fatalf("vehicle %s reported an implausible %.1f kph", o.VehicleID, o.SpeedKPH)
			}
			if o.BearingDeg < 0 || o.BearingDeg >= 360 {
				t.Fatalf("vehicle %s reported a bearing of %.1f degrees", o.VehicleID, o.BearingDeg)
			}
			if prev, ok := last[o.VehicleID]; ok {
				if o.Sequence != prev.Sequence+1 {
					t.Fatalf("vehicle %s went from sequence %d to %d; the counter is what makes a "+
						"gap in its reports detectable", o.VehicleID, prev.Sequence, o.Sequence)
				}
				if simulator.Haversine(
					simulator.Point{Lat: prev.Lat, Lon: prev.Lon},
					simulator.Point{Lat: o.Lat, Lon: o.Lon}) > 0.5 {
					moved++
				}
			}
			last[o.VehicleID] = o
		}
	}

	if moved == 0 {
		t.Error("no vehicle moved after a second of simulated time; the fleet is frozen")
	}
}

func TestParseGeometryRejectsBrokenInput(t *testing.T) {
	cases := []struct {
		name string
		doc  string
	}{
		{"no routes", `{"routes":[]}`},
		{"route without an id", `{"routes":[{"points":[[0,0],[1,1]]}]}`},
		{"route with one point", `{"routes":[{"id":"a","points":[[0,0]]}]}`},
		{"zero-length route", `{"routes":[{"id":"a","points":[[0,0],[0,0],[0,0]]}]}`},
		{"duplicate ids", `{"routes":[{"id":"a","points":[[0,0],[1,1]]},{"id":"a","points":[[2,2],[3,3]]}]}`},
		{"unknown field", `{"routes":[{"id":"a","points":[[0,0],[1,1]],"colour":"red"}]}`},
		{"malformed coordinate triple", `{"routes":[{"id":"a","points":[[0,0,0],[1,1]]}]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// A typo in this file changes what a benchmark measures, so every one of these has
			// to be a refusal rather than a silently ignored field.
			if _, err := simulator.ParseGeometry(strings.NewReader(tc.doc)); err == nil {
				t.Errorf("%s was accepted", tc.name)
			}
		})
	}
}

func TestPointAcceptsBothCoordinateForms(t *testing.T) {
	// The terse [lat, lon] form keeps the committed file readable; the object form is what a
	// reader inspecting the parsed value reaches for. Accepting both means the file's format is
	// not pinned by the parser.
	g, err := simulator.ParseGeometry(strings.NewReader(
		`{"routes":[{"id":"a","points":[[1.5,2.5],{"lat":3.5,"lon":4.5}]}]}`))
	if err != nil {
		t.Fatalf("ParseGeometry: %v", err)
	}
	pts := g.Routes[0].Points
	if pts[0].Lat != 1.5 || pts[0].Lon != 2.5 {
		t.Errorf("pair form parsed as %+v", pts[0])
	}
	if pts[1].Lat != 3.5 || pts[1].Lon != 4.5 {
		t.Errorf("object form parsed as %+v", pts[1])
	}
}

func TestNewFleetRefusesAnImpossibleRequest(t *testing.T) {
	g, err := simulator.LoadDefault()
	if err != nil {
		t.Fatalf("LoadDefault: %v", err)
	}
	if _, err := simulator.NewFleet(nil, simulator.FleetConfig{Vehicles: 1, Seed: 1}); err == nil {
		t.Error("NewFleet accepted nil geometry")
	}
	if _, err := simulator.NewFleet(g, simulator.FleetConfig{Vehicles: 0, Seed: 1}); err == nil {
		t.Error("NewFleet accepted a fleet of zero vehicles; the stream would be empty and the " +
			"benchmark would measure nothing")
	}
}
