package testsupport

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/aditya0si/event-stream-platform/internal/platform/broker"
)

// CreateTopic makes a uniquely-named topic and removes it when the test finishes.
//
// The cleanup opens its own client and context. The first version of this helper borrowed the
// caller's client and registered the delete with t.Cleanup — which runs *after* the test body's
// deferred Close, so every delete was attempted through a closed client, failed, and was
// swallowed by a t.Logf. Three leaked topics in a local broker were the only evidence; the
// code read correctly at every line.
func CreateTopic(t *testing.T, seeds []string, prefix string, partitions int) string {
	t.Helper()

	name := prefix + "." + strings.ReplaceAll(uuid.NewString(), "-", "")[:16]

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	admin, err := broker.NewClient(ctx, broker.Options{SeedBrokers: seeds, ClientID: "esp-test-admin"})
	if err != nil {
		t.Fatalf("connect to broker at %v: %v", seeds, err)
	}
	defer admin.Close()

	if _, err := broker.EnsureTopics(ctx, admin, []broker.TopicSpec{
		{Name: name, Partitions: partitions, Retention: time.Hour},
	}); err != nil {
		t.Fatalf("create test topic %s: %v", name, err)
	}

	t.Cleanup(func() {
		// A separate context *and* a separate client: the test's own context is cancelled by
		// the time cleanup runs, and any client the test body owns may already be closed.
		dctx, dcancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer dcancel()

		cleanup, err := broker.NewClient(dctx, broker.Options{SeedBrokers: seeds, ClientID: "esp-test-cleanup"})
		if err != nil {
			t.Logf("cleanup could not reach the broker to delete %s: %v", name, err)
			return
		}
		defer cleanup.Close()

		if _, err := kadm.NewClient(cleanup).DeleteTopics(dctx, name); err != nil {
			t.Logf("could not delete test topic %s: %v", name, err)
			return
		}
		t.Logf("removed test topic %s", name)
	})

	return name
}
