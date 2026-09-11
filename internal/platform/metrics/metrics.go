// Package metrics owns the Prometheus collectors.
//
// The rule this package follows is stated in the design (NFR-10): no metric exists that
// nobody would query during an incident. Each collector below is here because it answers
// a question an operator actually asks — comments name the question.
//
// Two conventions are load-bearing:
//
//   - Every label set is bounded. Labels take values from fixed vocabularies (outcomes,
//     reasons), never from user data such as a vehicle ID or an event ID, because an
//     unbounded label set is a memory leak with a dashboard attached.
//   - Counters are incremented where the event happens, not where it is convenient.
//     A counter updated in a batch flush measures the flush, not the event.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Registry is the process-wide registry. A fresh one per process (rather than the
// default global) so tests can build a registry without cross-contamination.
func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return reg
}

// --- ingest ---

var (
	// IngestRequestsTotal answers "is anything reaching us at all, and is it working?"
	IngestRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ingest_requests_total",
		Help: "HTTP requests to the ingest API, by outcome.",
	}, []string{"outcome"}) // accepted | rejected | malformed | unauthorised | error

	// IngestEventsTotal counts events, not requests, because a batch endpoint's request
	// rate says almost nothing about its throughput.
	IngestEventsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ingest_events_total",
		Help: "Individual telemetry events, by outcome.",
	}, []string{"outcome"}) // produced | rejected

	// IngestProduceDuration answers "is the broker the bottleneck?" — the single most
	// useful latency measurement on this path.
	IngestProduceDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "ingest_produce_seconds",
		Help:    "Time to publish one batch to the log.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5},
	})

	IngestBatchSize = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "ingest_batch_size",
		Help:    "Events per accepted batch.",
		Buckets: []float64{1, 10, 50, 100, 250, 500},
	})
)

// --- consumer ---

var (
	// ConsumerEventsTotal is the dedup ledger: produced vs applied vs duplicate vs late
	// vs dead. "effectively-once" is a claim that duplicates exist and are absorbed, so
	// the duplicate count is evidence rather than an embarrassment.
	ConsumerEventsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "consumer_events_total",
		Help: "Events handled by the consumer, by result.",
	}, []string{"result"}) // applied | duplicate | late | dead | retried

	// ConsumerLag answers "are we falling behind?" — the question that distinguishes a
	// healthy pipeline from one that is quietly accumulating a backlog.
	ConsumerLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "consumer_lag_messages",
		Help: "Estimated messages behind the log head, per partition.",
	}, []string{"partition"})

	// ConsumerTxDuration answers "where is the time going?" — broker, database, or fan-out.
	ConsumerTxDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "consumer_tx_seconds",
		Help:    "Time to process one event's transaction.",
		Buckets: []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5},
	})

	// ConsumerRetriesTotal answers "is a receiver failing?" before the DLQ fills up.
	ConsumerRetriesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "consumer_retries_total",
		Help: "Retry attempts, by reason.",
	}, []string{"reason"})

	ConsumerBatchSize = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "consumer_batch_size",
		Help:    "Records per poll.",
		Buckets: []float64{1, 5, 10, 50, 100, 200, 500},
	})
)

// --- dead letters ---

var (
	// DeadLetterTotal answers "how much is being thrown away?" — which should be zero,
	// and any nonzero value is a page.
	DeadLetterTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dead_letter_total",
		Help: "Events moved to the dead-letter queue, by reason.",
	}, []string{"reason"}) // decode | unknown_version | retries_exhausted

	// DLQDepth is a gauge rather than a counter because "is anything waiting for an
	// operator?" is a state question, and it must report zero rather than being absent
	// when the queue is empty. (The sibling project learned this the hard way: a GROUP BY
	// over an empty table produces no series, so the one health signal an operator had
	// vanished exactly when the system was healthy.)
	DLQDepth = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "dead_letter_depth",
		Help: "Rows awaiting replay.",
	})
)

// --- gateway / fan-out ---

var (
	GatewayClients = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "gateway_clients",
		Help: "Currently connected SSE clients.",
	})

	GatewayEventsSent = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "gateway_events_sent_total",
		Help: "SSE events written to clients.",
	})

	// GatewayDisconnectsTotal is the backpressure policy made visible (ADR-004). A shed
	// client is an expected, recoverable event — not an error — so it is counted by
	// reason rather than logged as a failure.
	GatewayDisconnectsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_disconnects_total",
		Help: "Client disconnections, by reason.",
	}, []string{"reason"}) // slow_consumer | client_closed | server_shutdown | write_error

	GatewayReplayEvents = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "gateway_replay_events",
		Help:    "Events replayed to a reconnecting client.",
		Buckets: []float64{0, 1, 10, 50, 100, 500, 1000},
	})

	// FanOutPublishDuration answers "is Redis slow?" — the question that separates a
	// stalled bus from a stalled consumer.
	FanOutPublishDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "fanout_publish_seconds",
		Help:    "Time to publish one event to the fan-out bus.",
		Buckets: []float64{0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.05},
	})

	// FanOutEventsTotal is the count of frames offered to the bus and what became of them.
	//
	// It is a separate counter from the consumer's own outcomes because the two can differ,
	// and the difference is informative: an event applied to the sink whose fan-out publish
	// failed is still perfectly durable, and the live view is one frame behind until the next
	// event. Without this counter that gap is invisible.
	FanOutEventsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "fanout_events_total",
		Help: "Frames published to the fan-out bus, by result.",
	}, []string{"result"}) // published | failed | no_subscribers
)

// --- dependencies ---

var (
	// BrokerUp / RedisUp / DBUp are gauges so that a dependency being down is a value the
	// dashboard shows, rather than a gap in a series that has to be noticed.
	BrokerUp = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "broker_up", Help: "1 when the log is reachable.",
	})
	RedisUp = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "redis_up", Help: "1 when Redis is reachable.",
	})
	DBUp = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "db_up", Help: "1 when Postgres is reachable.",
	})
)

// Register adds every collector in this package to a registry. Called once at startup by
// each binary; a duplicate registration panics, which is the desired behaviour — two
// registrations would double-count.
func Register(reg *prometheus.Registry) {
	reg.MustRegister(
		IngestRequestsTotal, IngestEventsTotal, IngestProduceDuration, IngestBatchSize,
		ConsumerEventsTotal, ConsumerLag, ConsumerTxDuration, ConsumerRetriesTotal, ConsumerBatchSize,
		DeadLetterTotal, DLQDepth,
		GatewayClients, GatewayEventsSent, GatewayDisconnectsTotal, GatewayReplayEvents,
		FanOutPublishDuration, FanOutEventsTotal,
		BrokerUp, RedisUp, DBUp,
	)
}
