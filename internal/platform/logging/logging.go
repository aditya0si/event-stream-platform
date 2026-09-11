// Package logging builds the structured logger every binary shares.
//
// The redaction here is not decoration. An event pipeline handles payloads that may
// contain personal data, and a log line is the easiest place for it to escape: an
// attribute named "payload" or "authorization" that reaches a log aggregator is a data
// leak with a retention policy attached. Keys are filtered centrally so no call site has
// to remember.
package logging

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

// redactedKeys are attribute keys whose values never reach a log line. Matching is
// case-insensitive and substring-based, so "db_password", "X-Auth-Token", and
// "authorization" are all caught by their common parts.
var redactedKeys = []string{
	"password",
	"passwd",
	"secret",
	"token",
	"authorization",
	"api_key",
	"apikey",
	"cookie",
	"credential",
	"private_key",
	"dsn",
	"database_url",
	"redis_url",
	// Payload contents are event data, not diagnostic information: a fleet telemetry body
	// can carry anything a device sends. Log its identity and size, never its contents.
	"payload",
	"body",
}

const redacted = "[REDACTED]"

func shouldRedact(key string) bool {
	k := strings.ToLower(key)
	for _, r := range redactedKeys {
		if strings.Contains(k, r) {
			return true
		}
	}
	return false
}

// New returns a logger writing JSON to stdout, with the given minimum level.
//
// JSON rather than text even in development: the same configuration then works in a
// container and in a terminal, and the field names are stable enough to grep. Development
// readability is bought with the level, not the format.
func New(level slog.Level) *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
		// The source of a log line matters when several binaries share a log stream, which
		// is exactly the case in `docker compose logs`.
		AddSource: true,
	})
	return slog.New(&redactingHandler{inner: handler})
}

// redactingHandler wraps a handler and filters sensitive attributes before they reach it.
type redactingHandler struct{ inner slog.Handler }

func (h *redactingHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.inner.Enabled(ctx, l)
}

func (h *redactingHandler) Handle(ctx context.Context, r slog.Record) error {
	clean := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	r.Attrs(func(a slog.Attr) bool {
		clean.AddAttrs(filterAttr(a))
		return true
	})
	return h.inner.Handle(ctx, clean)
}

func (h *redactingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	clean := make([]slog.Attr, 0, len(attrs))
	for _, a := range attrs {
		clean = append(clean, filterAttr(a))
	}
	return &redactingHandler{inner: h.inner.WithAttrs(clean)}
}

func (h *redactingHandler) WithGroup(name string) slog.Handler {
	return &redactingHandler{inner: h.inner.WithGroup(name)}
}

// filterAttr redacts a sensitive value and recurses into groups, so a nested
// `slog.Group("request", slog.String("authorization", ...))` is filtered too rather than
// slipping through because only the leaf was checked.
func filterAttr(a slog.Attr) slog.Attr {
	if shouldRedact(a.Key) {
		return slog.String(a.Key, redacted)
	}
	if a.Value.Kind() == slog.KindGroup {
		group := a.Value.Group()
		clean := make([]slog.Attr, 0, len(group))
		for _, g := range group {
			clean = append(clean, filterAttr(g))
		}
		return slog.Attr{Key: a.Key, Value: slog.GroupValue(clean...)}
	}
	return a
}
