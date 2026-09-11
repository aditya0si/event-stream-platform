// Package simulator is the deterministic fleet: a set of vehicles moving along committed route
// geometry, producing observations as a pure function of a seed and a step count (ADR-008).
//
// # What "deterministic" buys, and what it does not
//
// The same seed produces the same positions, in the same order, on any machine. That is what
// makes the benchmark reproducible: two runs of `cmd/simulate -seed 42` drive the pipeline with
// byte-identical payloads apart from their timestamps, so a throughput difference between them
// is a difference in the system, not in the load.
//
// It does not make the stream *realistic in the way a live feed is*. Real telemetry has
// correlated congestion, signal loss, and clock skew. This has vehicles following polylines at
// smoothly varying speeds. The README says so in those words rather than implying the numbers
// describe a production fleet — the honest claim is that a deterministic synthetic stream of
// known shape drove the system at a known rate.
//
// # Time is passed in, never read
//
// Step takes a duration and returns observations; it never calls time.Now. That is what lets a
// test advance the fleet by an hour in microseconds, and it keeps the generator's output a
// function of its inputs alone. Whoever calls Step decides what "now" is.
package simulator

import (
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"sort"
	"strings"
)

//go:embed routes.json
var defaultRoutes embed.FS

// earthRadiusM is the mean radius used for the haversine distance. Every length in this package
// is derived from it, so the geometry is internally consistent even where it disagrees slightly
// with a more precise ellipsoid — which is irrelevant here, because the routes are hand-written.
const earthRadiusM = 6371008.8

// Point is one waypoint.
type Point struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

// UnmarshalJSON accepts both [lat, lon] pairs and {"lat":…,"lon":…} objects, so the committed
// file can stay terse without pinning the format forever.
func (p *Point) UnmarshalJSON(raw []byte) error {
	trimmed := strings.TrimSpace(string(raw))
	if strings.HasPrefix(trimmed, "[") {
		var pair []float64
		if err := json.Unmarshal(raw, &pair); err != nil {
			return err
		}
		if len(pair) != 2 {
			return fmt.Errorf("simulator: a coordinate pair needs exactly 2 numbers, got %d", len(pair))
		}
		p.Lat, p.Lon = pair[0], pair[1]
		return nil
	}
	type alias Point
	var a alias
	if err := json.Unmarshal(raw, &a); err != nil {
		return err
	}
	*p = Point(a)
	return nil
}

// Route is a closed loop the vehicles travel along.
type Route struct {
	ID     string  `json:"id"`
	Name   string  `json:"name"`
	Points []Point `json:"points"`

	// cumulative[i] is the distance in metres from the start to Points[i], and segments[i] is
	// the length of the segment from Points[i] to Points[i+1]. They are derived, not
	// serialized, and they are what make interpolation a binary search rather than a walk.
	cumulative []float64
	segments   []float64
	// length is the total distance around the loop, set by prepare.
	length float64
}

// Geometry is the committed set of routes.
type Geometry struct {
	Note   []string `json:"note"`
	Routes []Route  `json:"routes"`
}

// LoadDefault returns the embedded geometry.
func LoadDefault() (*Geometry, error) {
	raw, err := defaultRoutes.ReadFile("routes.json")
	if err != nil {
		// Only reachable if the embed directive and the file disagree, which is a build-time
		// mistake this makes loud rather than silent.
		return nil, fmt.Errorf("simulator: read embedded routes: %w", err)
	}
	return ParseGeometry(strings.NewReader(string(raw)))
}

// ParseGeometry reads a geometry document and prepares every route for interpolation.
func ParseGeometry(r io.Reader) (*Geometry, error) {
	var g Geometry
	dec := json.NewDecoder(r)
	// An unknown field is a typo, and in a file whose whole purpose is to be the reproducible
	// input to a measurement, a silently ignored field is a measurement of something else.
	dec.DisallowUnknownFields()
	if err := dec.Decode(&g); err != nil {
		return nil, fmt.Errorf("simulator: parse geometry: %w", err)
	}
	if len(g.Routes) == 0 {
		return nil, errors.New("simulator: the geometry contains no routes")
	}

	seen := make(map[string]bool, len(g.Routes))
	for i := range g.Routes {
		rt := &g.Routes[i]
		if rt.ID == "" {
			return nil, fmt.Errorf("simulator: route %d has no id; a vehicle cannot name where it is", i)
		}
		if seen[rt.ID] {
			return nil, fmt.Errorf("simulator: route id %q appears more than once", rt.ID)
		}
		seen[rt.ID] = true
		if len(rt.Points) < 2 {
			return nil, fmt.Errorf("simulator: route %q needs at least 2 points, has %d", rt.ID, len(rt.Points))
		}
		if err := rt.prepare(); err != nil {
			return nil, err
		}
	}
	return &g, nil
}

// prepare computes the cumulative distances. A route whose total length is zero is refused: a
// vehicle on it would divide by zero when it wrapped.
func (r *Route) prepare() error {
	n := len(r.Points)
	r.segments = make([]float64, 0, n)
	r.cumulative = make([]float64, n)
	total := 0.0
	for i := 0; i < n-1; i++ {
		d := Haversine(r.Points[i], r.Points[i+1])
		r.segments = append(r.segments, d)
		total += d
		r.cumulative[i+1] = total
	}
	if total <= 0 {
		return fmt.Errorf("simulator: route %q has zero length; every point is the same place", r.ID)
	}
	r.length = total
	return nil
}

// Length is the total distance around the route, in metres.
func (r *Route) Length() float64 { return r.length }

// At returns the position and heading at a distance along the route, wrapping at the end so a
// route behaves as a loop. Heading is degrees clockwise from north, which is what the vehicle
// payload carries and what a map arrow needs.
func (r *Route) At(offsetM float64) (Point, float64) {
	if r.length <= 0 {
		// Unreachable for a prepared route; returning the first point beats dividing by zero.
		return r.Points[0], 0
	}
	// Wrap into [0, length). math.Mod keeps the sign of the dividend, so a negative offset
	// (which a caller should not produce, but which must not panic) lands correctly too.
	off := math.Mod(offsetM, r.length)
	if off < 0 {
		off += r.length
	}

	// The last segment index whose cumulative distance is at or below the offset. A binary
	// search rather than a linear walk because a benchmark may step this millions of times.
	i := sort.SearchFloat64s(r.cumulative, off) - 1
	if i < 0 {
		i = 0
	}
	if i >= len(r.segments) {
		i = len(r.segments) - 1
	}

	seg := r.segments[i]
	if seg <= 0 {
		return r.Points[i], bearing(r.Points[i], r.Points[i+1])
	}
	t := (off - r.cumulative[i]) / seg
	if t < 0 {
		t = 0
	} else if t > 1 {
		t = 1
	}

	a, b := r.Points[i], r.Points[i+1]
	return Point{
		Lat: a.Lat + (b.Lat-a.Lat)*t,
		Lon: a.Lon + (b.Lon-a.Lon)*t,
	}, bearing(a, b)
}

// Vehicle is one simulated unit.
type Vehicle struct {
	ID     string
	Route  *Route
	Offset float64 // metres along its route
	// BaseSpeedKPH is the vehicle's free-flow speed. The instantaneous speed varies around it
	// so the stream is not a wall of identical numbers, while remaining a pure function of the
	// seed.
	BaseSpeedKPH float64
	// phase shifts the speed oscillation so vehicles on one route do not pulse in unison.
	phase float64
	// Sequence is this vehicle's own monotonic counter, and it is the field the sink stores so
	// a gap in a vehicle's reports is detectable without relying on timestamps.
	Sequence uint64
	// elapsed tracks simulated time, which is what the speed oscillation is a function of.
	elapsed float64
}

// Observation is one report from one vehicle.
type Observation struct {
	VehicleID  string
	RouteID    string
	Lat        float64
	Lon        float64
	SpeedKPH   float64
	BearingDeg float64
	Sequence   uint64
}

// Fleet is a set of vehicles over a geometry.
type Fleet struct {
	geometry *Geometry
	vehicles []*Vehicle
	elapsed  float64

	// out is the observation slice Step reuses between calls: grown once, then written in
	// place. See Step's comment for why it is not allocated per call.
	out []Observation
}

// FleetConfig configures a fleet.
type FleetConfig struct {
	// Seed makes the fleet reproducible. Two fleets with the same seed, geometry and size are
	// identical for the same number of steps.
	Seed int64
	// Vehicles is how many units to place. They are distributed across the routes in a fixed
	// order, so adding a vehicle does not reshuffle the existing ones.
	Vehicles int
}

// NewFleet places vehicles. Placement is derived from the seed, so a run is reproducible from
// its command line alone.
func NewFleet(g *Geometry, cfg FleetConfig) (*Fleet, error) {
	if g == nil || len(g.Routes) == 0 {
		return nil, errors.New("simulator: no geometry")
	}
	if cfg.Vehicles < 1 {
		return nil, fmt.Errorf("simulator: a fleet needs at least 1 vehicle, got %d", cfg.Vehicles)
	}

	// Deterministic placement. rand.NewSource is not safe for concurrent use, but a Fleet is
	// single-goroutine by design: the simulator's own loop owns it.
	rng := rand.New(rand.NewSource(cfg.Seed)) //nolint:gosec // determinism is the requirement, not unpredictability

	f := &Fleet{geometry: g, vehicles: make([]*Vehicle, 0, cfg.Vehicles)}
	for i := 0; i < cfg.Vehicles; i++ {
		// Round-robin with an offset, so vehicle 0 and vehicle 4 are on different routes rather
		// than every fourth vehicle clustering on one.
		route := &g.Routes[(i+int(rng.Int63n(int64(len(g.Routes)))))%len(g.Routes)]

		v := &Vehicle{
			ID:    fmt.Sprintf("veh-%04d", i+1),
			Route: route,
			// Spread along the loop rather than all starting at the first waypoint, which
			// would put the whole fleet on one point for the first seconds of a demo.
			Offset: rng.Float64() * route.Length(),
			// 15–85 kph: a plausible urban band, wide enough that the viewer's speed palette
			// shows variation rather than one colour.
			BaseSpeedKPH: 15 + rng.Float64()*70,
			phase:        rng.Float64() * 2 * math.Pi,
		}
		f.vehicles = append(f.vehicles, v)
	}
	return f, nil
}

// Vehicles exposes the fleet for reporting. The slice is the fleet's own; callers must not
// mutate it.
func (f *Fleet) Vehicles() []*Vehicle { return f.vehicles }

// Routes returns the geometry's routes.
func (f *Fleet) Routes() []Route { return f.geometry.Routes }

// Step advances the fleet by dt and returns one observation per vehicle.
//
// The returned slice is reused between calls, so a caller that needs to retain observations
// must copy them. That is deliberate: at 2,000 events/s, allocating a fresh slice per step is
// the difference between measuring the pipeline and measuring the garbage collector.
func (f *Fleet) Step(dt float64) []Observation {
	if len(f.out) < len(f.vehicles) {
		f.out = make([]Observation, len(f.vehicles))
	}
	out := f.out[:len(f.vehicles)]

	for i, v := range f.vehicles {
		v.elapsed += dt
		speed := v.speedAt(v.elapsed)
		v.Offset += speed / 3.6 * dt // kph -> m/s
		pos, heading := v.Route.At(v.Offset)
		v.Sequence++

		out[i] = Observation{
			VehicleID:  v.ID,
			RouteID:    v.Route.ID,
			Lat:        pos.Lat,
			Lon:        pos.Lon,
			SpeedKPH:   math.Round(speed*10) / 10,
			BearingDeg: math.Round(heading*10) / 10,
			Sequence:   v.Sequence,
		}
	}
	f.elapsed += dt
	return out
}

// Elapsed is the simulated time the fleet has advanced through.
func (f *Fleet) Elapsed() float64 { return f.elapsed }

// speedAt is the vehicle's instantaneous speed: its base, modulated by a slow oscillation so
// the stream looks alive while staying a pure function of simulated time and the seed.
//
// The oscillation never reaches zero and never doubles the base, which keeps vehicles moving —
// a fleet that stops moving is a stream of identical positions, and the pipeline's behaviour
// on that stream would say more about the generator than about the system.
func (v *Vehicle) speedAt(elapsed float64) float64 {
	wave := math.Sin(elapsed/20 + v.phase)
	return v.BaseSpeedKPH * (1 + 0.25*wave)
}

// Haversine returns the great-circle distance between two points, in metres.
func Haversine(a, b Point) float64 {
	lat1, lat2 := a.Lat*math.Pi/180, b.Lat*math.Pi/180
	dLat := (b.Lat - a.Lat) * math.Pi / 180
	dLon := (b.Lon - a.Lon) * math.Pi / 180
	h := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1)*math.Cos(lat2)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return 2 * earthRadiusM * math.Asin(math.Min(1, math.Sqrt(h)))
}

// bearing returns the initial compass bearing from a to b, in degrees clockwise from north.
func bearing(a, b Point) float64 {
	lat1, lat2 := a.Lat*math.Pi/180, b.Lat*math.Pi/180
	dLon := (b.Lon - a.Lon) * math.Pi / 180
	y := math.Sin(dLon) * math.Cos(lat2)
	x := math.Cos(lat1)*math.Sin(lat2) - math.Sin(lat1)*math.Cos(lat2)*math.Cos(dLon)
	if x == 0 && y == 0 {
		return 0
	}
	return math.Mod(math.Atan2(y, x)*180/math.Pi+360, 360)
}
