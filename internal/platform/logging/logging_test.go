package logging_test

import (
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/aditya0si/event-stream-platform/internal/platform/logging"
)

// capture runs fn with os.Stdout redirected to a pipe and returns what was written.
//
// Redirecting stdout rather than adding an injection seam to the production API is
// deliberate. logging.New binds os.Stdout itself, and this exercises that exact path —
// including the AddSource setting and the JSON handler — rather than a test-only
// arrangement that could drift from it. The consequence is that stdout must be swapped
// *before* the logger is constructed, since the handler captures the writer at that moment.
func capture(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	old := os.Stdout
	os.Stdout = w
	defer func() { os.Stdout = old }()

	fn()

	if err := w.Close(); err != nil {
		t.Fatalf("close pipe: %v", err)
	}
	os.Stdout = old

	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read pipe: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("close read end: %v", err)
	}
	return string(out)
}

// TestSecretsNeverReachTheLog is the test that matters most in this file. Each value below
// would be a real incident if it appeared in a log aggregator, and the point of the
// redacting handler is that no call site has to remember to filter them.
func TestSecretsNeverReachTheLog(t *testing.T) {
	const (
		bearer = "Bearer eyJhbGciOiJIUzI1NiJ9.header-payload-signature"
		passwd = "correct-horse-battery-staple"
		plate  = "KA01AB1234"
	)

	out := capture(t, func() {
		log := logging.New(slog.LevelDebug)
		log.Info("event ingested",
			slog.String("authorization", bearer),
			slog.String("db_password", passwd),
			slog.String("payload", `{"plate":"`+plate+`"}`),
			slog.String("vehicle_id", "v-42"),
		)
	})

	for _, secret := range []string{bearer, passwd, plate} {
		if strings.Contains(out, secret) {
			t.Errorf("a sensitive value reached the log: %q\nlog line: %s", secret, out)
		}
	}
	if !strings.Contains(out, "[REDACTED]") {
		t.Errorf("expected redaction markers in the output:\n%s", out)
	}
	// Redaction must not be so blunt that the log stops being useful: the identifiers an
	// operator actually needs have to survive.
	if !strings.Contains(out, "v-42") {
		t.Errorf("an ordinary key was redacted, which would make logs useless:\n%s", out)
	}
	if !strings.Contains(out, "event ingested") {
		t.Errorf("the message itself was lost:\n%s", out)
	}
}

// TestRedactionCoversNestedGroups covers the failure mode where only top-level keys are
// checked and a group becomes a way to smuggle a token through.
func TestRedactionCoversNestedGroups(t *testing.T) {
	out := capture(t, func() {
		log := logging.New(slog.LevelDebug)
		log.Info("upstream call",
			slog.Group("request",
				slog.String("Token", "tok_live_abc123"),
				slog.Int("attempt", 3),
			),
		)
	})

	if strings.Contains(out, "tok_live_abc123") {
		t.Errorf("a nested group leaked a token:\n%s", out)
	}
	if !strings.Contains(out, `"attempt":3`) {
		t.Errorf("a non-sensitive group member must survive:\n%s", out)
	}
}

// TestRedactionIsCaseInsensitive: the same key arrives spelled differently depending on
// who wrote the call site ("Authorization" from a header map, "API_KEY" from configuration).
func TestRedactionIsCaseInsensitive(t *testing.T) {
	out := capture(t, func() {
		log := logging.New(slog.LevelDebug)
		log.Info("headers",
			slog.String("Authorization", "leak-marker-one"),
			slog.String("API_KEY", "leak-marker-two"),
			slog.String("Cookie", "leak-marker-three"),
		)
	})

	for _, marker := range []string{"leak-marker-one", "leak-marker-two", "leak-marker-three"} {
		if strings.Contains(out, marker) {
			t.Errorf("%s was not redacted:\n%s", marker, out)
		}
	}
}
