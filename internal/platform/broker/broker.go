// Package broker wraps the Kafka wire client so that the rest of the code does not import
// franz-go directly.
//
// The wrapper is thin on purpose — it does not abstract the log, it just gives this
// module one place to configure a client, one place to ping it, and one place to create
// topics. A thicker abstraction would hide the properties this project exists to
// demonstrate (partitions, keys, offsets, consumer groups), which would be the wrong kind
// of decoupling.
package broker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Options configures a client.
type Options struct {
	SeedBrokers []string
	// ClientID appears in the broker's own logs and in JMX-style metrics, so it should
	// name the process.
	ClientID string
}

// NewClient builds a client and verifies it can reach at least one seed broker.
//
// The ping matters for the same reason the database ping does: a process that starts
// with an unreachable broker and only fails on its first produce is a process that
// reports ready while being unable to do its job.
func NewClient(ctx context.Context, opts Options) (*kgo.Client, error) {
	if len(opts.SeedBrokers) == 0 {
		return nil, errors.New("broker: no seed brokers configured")
	}
	clientID := opts.ClientID
	if clientID == "" {
		clientID = "event-stream-platform"
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(opts.SeedBrokers...),
		kgo.ClientID(clientID),
		// A bounded retry policy rather than franz-go's default of "keep trying forever":
		// the ingest handler has an HTTP request waiting, and a request that hangs for
		// minutes is worse than one that fails with a 503 and lets the client retry.
		kgo.RecordRetries(3),
		kgo.RetryTimeout(10*time.Second),
		// Deliver reports per-record errors to the producer's callback rather than
		// blocking; the producer decides what a failure means.
		kgo.ProducerBatchMaxBytes(1<<20),
	)
	if err != nil {
		return nil, fmt.Errorf("broker: create client: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := cl.Ping(pingCtx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("broker: ping %v failed: %w", opts.SeedBrokers, err)
	}
	return cl, nil
}

// Ping checks that the broker is still reachable. It backs the readiness probe, so it is
// called on a timer and must not do real work.
func Ping(ctx context.Context, cl *kgo.Client) error {
	if cl == nil {
		return errors.New("broker: nil client")
	}
	if err := cl.Ping(ctx); err != nil {
		return fmt.Errorf("broker: ping failed: %w", err)
	}
	return nil
}

// TopicSpec describes a topic this system needs.
type TopicSpec struct {
	Name       string
	Partitions int
	Retention  time.Duration
}

// EnsureTopics creates topics that do not exist and leaves existing ones alone.
//
// It is idempotent because it is called from the migrate container, which may run
// repeatedly (every `docker compose up`, every CI run). Recreating a topic that already
// holds data would silently destroy the stream, so this checks first and never deletes or
// reconfigures what it finds.
//
// Each topic is created in its own request because the Kafka create-topics call carries
// one partition count and one configuration set for every topic it names. The two topics
// this system needs differ in both, so batching them would silently give one of them the
// other's shape.
func EnsureTopics(ctx context.Context, cl *kgo.Client, specs []TopicSpec) (created []string, err error) {
	if cl == nil {
		return nil, errors.New("broker: nil client")
	}
	adm := kadm.NewClient(cl)

	existing, err := adm.ListTopics(ctx)
	if err != nil {
		return nil, fmt.Errorf("broker: list topics: %w", err)
	}

	for _, spec := range specs {
		if _, ok := existing[spec.Name]; ok {
			continue
		}

		// Retention is expressed in milliseconds in the Kafka protocol. This is one of the
		// places where the wire format leaks into the wrapper, and it is better named here
		// than at three call sites.
		configs := map[string]*string{}
		if spec.Retention > 0 {
			ms := fmt.Sprintf("%d", spec.Retention.Milliseconds())
			configs["retention.ms"] = &ms
		}

		responses, err := adm.CreateTopics(ctx, int32(spec.Partitions), 1, configs, spec.Name)
		if err != nil {
			return created, fmt.Errorf("broker: create topic %q: %w", spec.Name, err)
		}
		for _, r := range responses {
			// A concurrent migrate run winning the race is not an error: it is the
			// outcome this function promises.
			if r.Err != nil && !errors.Is(r.Err, kerr.TopicAlreadyExists) {
				return created, fmt.Errorf("broker: create topic %q: %w", r.Topic, r.Err)
			}
			if r.Err == nil {
				created = append(created, r.Topic)
			}
		}
	}
	return created, nil
}

// DescribeTopics returns the partition counts of the named topics, for the readiness
// endpoint and the smoke test to assert against reality rather than against config.
func DescribeTopics(ctx context.Context, cl *kgo.Client, names ...string) (map[string]int, error) {
	if cl == nil {
		return nil, errors.New("broker: nil client")
	}
	adm := kadm.NewClient(cl)
	details, err := adm.ListTopics(ctx, names...)
	if err != nil {
		return nil, fmt.Errorf("broker: describe topics: %w", err)
	}
	out := make(map[string]int, len(names))
	for _, d := range details {
		if d.Err != nil {
			return nil, fmt.Errorf("broker: describe topic %q: %w", d.Topic, d.Err)
		}
		out[d.Topic] = len(d.Partitions)
	}
	return out, nil
}
