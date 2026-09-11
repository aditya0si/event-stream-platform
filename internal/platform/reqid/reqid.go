// Package reqid gives every request an identifier that follows it through the system.
//
// The identifier is what makes a log line attributable to a request, and a request traceable
// across the ingest process, the log, and the consumer. It is deliberately not a distributed
// trace: no span is created or exported (docs/DESIGN.md says so rather than implying
// otherwise). It is the cheap, honest half of that story — one string, propagated, logged.
package reqid

import (
	"context"
	"net/http"
	"strings"

	"github.com/aditya0si/event-stream-platform/internal/platform/idgen"
)

// Header is the header the id travels in.
const Header = "X-Request-Id"

// MaxLength bounds an inbound identifier. A caller-supplied value is echoed into logs, so an
// unbounded one is a way to write megabytes into a log aggregator from a single request.
const MaxLength = 64

type ctxKey struct{}

// FromContext returns the request id, or "" when there is none.
func FromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx.Value(ctxKey{}).(string); ok {
		return v
	}
	return ""
}

// WithID places an id in the context.
func WithID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// Middleware ensures every request carries an id, echoes it on the response, and puts it in
// the request context.
//
// An inbound X-Request-Id is honoured so a caller can correlate its own id with ours — a
// useful property for a client debugging a batch it sent. It is sanitised first, because
// echoing an attacker-controlled header into response headers and log lines is a small
// injection surface: anything outside the printable ASCII range, or longer than MaxLength, is
// replaced rather than trusted.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := sanitize(r.Header.Get(Header))
		if id == "" {
			id = idgen.New()
		}

		// Set on the response before the handler runs, so an early write (an error path)
		// still carries it without every call site remembering.
		w.Header().Set(Header, id)
		next.ServeHTTP(w, r.WithContext(WithID(r.Context(), id)))
	})
}

// sanitize accepts a conservative subset of characters: the id lands in logs and in a
// response header, and a newline in either is a forging opportunity.
func sanitize(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" || len(raw) > MaxLength {
		return ""
	}
	for _, c := range raw {
		switch {
		case c >= 'a' && c <= 'z',
			c >= 'A' && c <= 'Z',
			c >= '0' && c <= '9',
			c == '-', c == '_', c == '.':
			continue
		default:
			return ""
		}
	}
	return raw
}
