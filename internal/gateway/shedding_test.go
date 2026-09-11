package gateway

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/aditya0si/event-stream-platform/internal/fanout"
)

// These tests are in the package rather than outside it, and that is deliberate. The shedding
// policy is the one behaviour that cannot be tested honestly over HTTP: whether a client is
// dropped depends on a non-blocking send failing, and arranging that through a real socket means
// racing the client's read speed and the kernel's buffer size — a test that passes on a fast
// machine and flakes on a loaded CI runner. Reaching the send directly makes the policy
// deterministic, which is the whole point: a flaky test of a drop policy is worse than no test,
// because it trains you to ignore the failure that matters.

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

type stubSource struct{}

func (stubSource) Frames(context.Context) (<-chan fanout.Position, func(), error) {
	return make(chan fanout.Position), func() {}, nil
}

type stubHistory struct{}

func (stubHistory) PositionTime(context.Context, string) (time.Time, bool, error) {
	return time.Time{}, false, nil
}

func (stubHistory) PositionsSince(context.Context, time.Time, string, int) ([]fanout.Position, error) {
	return nil, nil
}

func newTestGateway(t *testing.T, opts Options) *Gateway {
	t.Helper()
	if opts.Log == nil {
		opts.Log = quietLogger()
	}
	g, err := New(stubSource{}, stubHistory{}, opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return g
}

func TestBroadcast_ShedsAClientThatCannotKeepUp(t *testing.T) {
	g := newTestGateway(t, Options{ClientBuffer: 1, MaxClients: 4})

	c := &client{out: make(chan fanout.Position, 1), remote: "test-client"}
	g.mu.Lock()
	g.clients[c] = struct{}{}
	g.mu.Unlock()

	// Fill the buffer, then offer one more frame. The offer is non-blocking by design, so it
	// fails and the client is shed instead of stalling the fan-out loop for everyone else.
	c.out <- fanout.Position{EventID: "fills-the-buffer"}
	g.broadcast(fanout.Position{EventID: "one-too-many"})

	g.mu.Lock()
	_, still := g.clients[c]
	g.mu.Unlock()
	if still {
		t.Fatal("a client whose buffer is full was kept: this is the unbounded growth ADR-004 exists to prevent")
	}
	if n := g.SubscriberCount(); n != 0 {
		t.Errorf("SubscriberCount = %d, want 0", n)
	}

	// The channel must be closed, or the per-client goroutine would never end and the stream
	// would hang instead of reconnecting. The buffered frame drains first, then the receive
	// reports closed.
	<-c.out
	if _, open := <-c.out; open {
		t.Error("the shed client's channel is still open, so its stream goroutine would never finish")
	}
}

func TestBroadcast_KeepsAClientThatCanAccept(t *testing.T) {
	g := newTestGateway(t, Options{ClientBuffer: 4, MaxClients: 4})

	c := &client{out: make(chan fanout.Position, 4), remote: "test-client"}
	g.mu.Lock()
	g.clients[c] = struct{}{}
	g.mu.Unlock()

	g.broadcast(fanout.Position{EventID: "evt-1"})

	g.mu.Lock()
	_, still := g.clients[c]
	g.mu.Unlock()
	if !still {
		t.Fatal("a client with room in its buffer was shed")
	}

	select {
	case p := <-c.out:
		if p.EventID != "evt-1" {
			t.Errorf("delivered %q, want evt-1", p.EventID)
		}
	default:
		t.Error("the frame was not delivered to a client that had room")
	}
}

func TestNew_RejectsIncompleteConfiguration(t *testing.T) {
	log := quietLogger()

	if _, err := New(nil, stubHistory{}, Options{ClientBuffer: 1, MaxClients: 1, Log: log}); err == nil {
		t.Error("New accepted a nil frame source")
	}
	// A nil history is refused rather than tolerated: a reconnect could not be resumed, and the
	// gateway would answer a reconnecting viewer with silence.
	if _, err := New(stubSource{}, nil, Options{ClientBuffer: 1, MaxClients: 1, Log: log}); err == nil {
		t.Error("New accepted a nil history source")
	}
	if _, err := New(stubSource{}, stubHistory{}, Options{ClientBuffer: 1, MaxClients: 1}); err == nil {
		t.Error("New accepted a nil logger")
	}
	if _, err := New(stubSource{}, stubHistory{}, Options{ClientBuffer: 0, MaxClients: 1, Log: log}); err == nil {
		t.Error("New accepted a zero client buffer, which would mean unbounded growth or no delivery")
	}
	if _, err := New(stubSource{}, stubHistory{}, Options{ClientBuffer: 1, MaxClients: 0, Log: log}); err == nil {
		t.Error("New accepted zero max clients, which would mean no bound at all")
	}
}
