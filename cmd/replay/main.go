// Command replay is the operator's tool for the dead-letter queue: list what the consumer
// refused, inspect one entry, and put an entry back on the log it came from.
//
// It is a CLI rather than an HTTP endpoint deliberately. The recovery path must not depend on
// the process that failed — and the consumer being down is exactly when an operator reaches for
// this. An endpoint on the consumer would be unreachable precisely then; an endpoint on the
// ingest service would be a second write path into the log with its own auth story.
//
// Usage:
//
//	replay list   [--state dead|replayed|all] [--limit N]
//	replay show   --event-id <id>
//	replay replay --event-id <id>
//	replay replay --all [--limit N]
//
// Exit codes are what make it scriptable: 0 for success, 1 for a failure that was reported, and
// 2 for a usage error — so a wrapper can distinguish "the queue is empty" from "you typed the
// command wrong".
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aditya0si/event-stream-platform/internal/platform/broker"
	"github.com/aditya0si/event-stream-platform/internal/platform/config"
	"github.com/aditya0si/event-stream-platform/internal/platform/db"
	"github.com/aditya0si/event-stream-platform/internal/replay"
)

func main() {
	os.Exit(run())
}

func run() int {
	// Args are checked before config is loaded, so `replay help` works on a machine with no
	// DATABASE_URL set. A usage error should not require a working deployment to explain
	// itself.
	if len(os.Args) < 2 {
		usage()
		return 2
	}
	cmd := os.Args[1]
	if cmd == "help" || cmd == "-h" || cmd == "--help" {
		usage()
		return 0
	}

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "replay: %v\n", err)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startupCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	pool, err := db.Open(startupCtx, db.Options{
		URL:             cfg.Postgres.URL,
		MaxConns:        cfg.Postgres.MaxConns,
		MinConns:        cfg.Postgres.MinConns,
		MaxConnLifetime: cfg.Postgres.MaxConnLifetime,
		ConnectTimeout:  cfg.Postgres.ConnectTimeout,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "replay: %v\n", err)
		return 1
	}
	defer pool.Close()

	store, err := replay.NewStore(pool)
	if err != nil {
		fmt.Fprintf(os.Stderr, "replay: %v\n", err)
		return 1
	}

	switch cmd {
	case "list":
		return cmdList(ctx, store)
	case "show":
		return cmdShow(ctx, store)
	case "replay":
		return cmdReplay(ctx, cfg, store)
	default:
		fmt.Fprintf(os.Stderr, "replay: unknown command %q\n\n", cmd)
		usage()
		return 2
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `replay — inspect and replay refused events

  replay list   [--state dead|replayed|all] [--limit N]
        What the consumer refused, most recently failed first.

  replay show   --event-id <id>
        One entry in full, including the stored payload.

  replay replay --event-id <id>
  replay replay --all [--limit N]
        Put the original bytes back on the topic they came from. The ordinary consumer
        processes them again, so replay inherits the same deduplication guarantee:
        an event that had already been applied is absorbed rather than applied twice.

Environment: DATABASE_URL (required), BROKER_SEEDS for replay.
`)
}

func cmdList(ctx context.Context, store *replay.Store) int {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	state := fs.String("state", "dead", "dead, replayed, or all")
	limit := fs.Int("limit", replay.DefaultListLimit, "maximum entries to show")
	if err := fs.Parse(os.Args[2:]); err != nil {
		return 2
	}

	entries, err := store.List(ctx, replay.Filter{State: *state, Limit: *limit})
	if err != nil {
		fmt.Fprintf(os.Stderr, "replay: %v\n", err)
		return 1
	}
	if len(entries) == 0 {
		// Not an error. "Nothing is waiting" is the healthy answer, and returning non-zero
		// would make a monitoring script report a problem on a quiet system.
		fmt.Printf("no dead letters in state %q\n", *state)
		return 0
	}

	w := newTableWriter()
	w.row("EVENT ID", "REASON", "ATTEMPTS", "LAST FAILED", "ORIGIN", "ERROR")
	for _, e := range entries {
		w.row(e.EventID, e.Reason, fmt.Sprint(e.Attempts),
			e.LastFailedAt.UTC().Format(time.RFC3339),
			fmt.Sprintf("%s/%d/%d", e.Topic, e.Partition, e.Offset),
			firstLine(e.Error, 60))
	}
	w.flush()
	fmt.Printf("\n%d entr%s\n", len(entries), plural(len(entries)))
	return 0
}

func cmdShow(ctx context.Context, store *replay.Store) int {
	fs := flag.NewFlagSet("show", flag.ContinueOnError)
	eventID := fs.String("event-id", "", "the event to show")
	if err := fs.Parse(os.Args[2:]); err != nil {
		return 2
	}
	if *eventID == "" {
		fmt.Fprintln(os.Stderr, "replay show: --event-id is required")
		return 2
	}

	e, err := store.Get(ctx, *eventID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "replay: %v\n", err)
		return 1
	}

	fmt.Printf("event_id        %s\n", e.EventID)
	fmt.Printf("state           %s\n", e.State)
	fmt.Printf("reason          %s\n", e.Reason)
	fmt.Printf("attempts        %d\n", e.Attempts)
	fmt.Printf("origin          %s partition %d offset %d\n", e.Topic, e.Partition, e.Offset)
	fmt.Printf("first failed    %s\n", e.FirstFailedAt.UTC().Format(time.RFC3339))
	fmt.Printf("last failed     %s\n", e.LastFailedAt.UTC().Format(time.RFC3339))
	if e.ReplayedAt != nil {
		fmt.Printf("replayed at     %s\n", e.ReplayedAt.UTC().Format(time.RFC3339))
	}
	fmt.Printf("error           %s\n", e.Error)
	fmt.Printf("\npayload (%d bytes)\n", len(e.Payload))

	// Pretty-print when the payload is JSON, and print it raw when it is not. A payload that
	// failed to decode is the common case here, so the raw form is the one that has to work.
	var pretty any
	if json.Unmarshal(e.Payload, &pretty) == nil {
		out, err := json.MarshalIndent(pretty, "", "  ")
		if err == nil {
			fmt.Println(string(out))
			return 0
		}
	}
	fmt.Println(string(e.Payload))
	return 0
}

func cmdReplay(ctx context.Context, cfg config.Config, store *replay.Store) int {
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	eventID := fs.String("event-id", "", "replay one event")
	all := fs.Bool("all", false, "replay every entry currently in the 'dead' state")
	limit := fs.Int("limit", replay.DefaultListLimit, "with --all, the maximum to replay")
	if err := fs.Parse(os.Args[2:]); err != nil {
		return 2
	}

	if *eventID == "" && !*all {
		fmt.Fprintln(os.Stderr, "replay replay: give either --event-id or --all")
		return 2
	}
	if *eventID != "" && *all {
		fmt.Fprintln(os.Stderr, "replay replay: --event-id and --all are mutually exclusive")
		return 2
	}

	// The broker is only needed for this subcommand, so it is connected here rather than in
	// run(): `list` and `show` work on a host that cannot reach a broker at all.
	pub, err := broker.NewProducer(ctx, broker.Options{
		SeedBrokers: cfg.Broker.SeedBrokers,
		ClientID:    "esp-replay",
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "replay: %v\n", err)
		return 1
	}
	defer pub.Close()

	rep, err := replay.NewReplayer(store, pub)
	if err != nil {
		fmt.Fprintf(os.Stderr, "replay: %v\n", err)
		return 1
	}

	if *eventID != "" {
		res, err := rep.Replay(ctx, *eventID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "replay: %v\n", err)
			return 1
		}
		fmt.Printf("republished %s to %s (origin partition %d offset %d)\n",
			res.Entry.EventID, res.Entry.Topic, res.Entry.Partition, res.Entry.Offset)
		fmt.Println("the consumer will process it again; a previously applied event is absorbed by its deduplication key")
		return 0
	}

	results, errs, err := rep.ReplayAll(ctx, *limit)
	if err != nil {
		fmt.Fprintf(os.Stderr, "replay: %v\n", err)
		return 1
	}
	for _, r := range results {
		fmt.Printf("republished %s (was %s attempts, reason %s)\n",
			r.Entry.EventID, fmt.Sprint(r.Entry.Attempts), r.Entry.Reason)
	}
	for _, e := range errs {
		fmt.Fprintf(os.Stderr, "  failed: %v\n", e)
	}
	fmt.Printf("\n%d republished, %d failed\n", len(results), len(errs))

	// A partial failure is reported as a failure. An operator draining a queue needs to know
	// whether the run was clean, and "some of it worked" is not the same answer as "done".
	if len(errs) > 0 {
		return 1
	}
	return 0
}

// logger is deliberately absent from this command: its output is stdout for a human or a
// script, not structured logs, and a CLI whose report goes through a log handler is one nobody
// can pipe.

func firstLine(s string, max int) string {
	for i, r := range s {
		if r == '\n' {
			s = s[:i]
			break
		}
	}
	if len(s) > max {
		return s[:max] + "…"
	}
	return s
}

func plural(n int) string {
	if n == 1 {
		return "y"
	}
	return "ies"
}

// tableWriter pads columns to their widest cell so the output is readable without a terminal
// table library. It buffers rows because column widths are only known once every cell is seen.
type tableWriter struct {
	header []string
	rows   [][]string
	widths []int
}

func newTableWriter() *tableWriter { return &tableWriter{} }

func (t *tableWriter) row(cells ...string) {
	if t.header == nil {
		t.header = cells
		for _, c := range cells {
			t.widths = append(t.widths, len(c))
		}
		return
	}
	for i, c := range cells {
		if i < len(t.widths) && len(c) > t.widths[i] {
			t.widths[i] = len(c)
		}
	}
	t.rows = append(t.rows, cells)
}

func (t *tableWriter) flush() {
	printRow := func(cells []string) {
		for i, c := range cells {
			if i == len(cells)-1 {
				fmt.Print(c)
				break
			}
			fmt.Printf("%-*s  ", t.widths[i], c)
		}
		fmt.Println()
	}
	printRow(t.header)
	for _, r := range t.rows {
		printRow(r)
	}
}
