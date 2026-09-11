// Package idgen mints the identifiers this system hands out: event IDs and request IDs.
//
// UUIDv7 is used for both because it is time-ordered. That property matters more here
// than in a request/response service: event IDs are the deduplication key, so a table
// keyed by them is appended in roughly insertion order and its index stays compact,
// whereas random UUIDv4 keys scatter writes across the whole b-tree. It also means a
// human reading a log line can tell when an ID was minted, without a second lookup.
//
// IDs are generated in the application rather than by the database, for the reason the
// sibling repository records in its ADR-007: the producer must know an event's identity
// *before* it is written, because that identity is what makes a retry idempotent.
package idgen

import (
	"crypto/rand"
	"fmt"

	"github.com/google/uuid"
)

// New returns a UUIDv7 as a string.
//
// It panics only if the system's entropy source fails, which is not a condition this
// process can meaningfully recover from — every identifier it mints would be worthless.
func New() string {
	u, err := uuid.NewV7()
	if err != nil {
		panic(fmt.Sprintf("idgen: crypto/rand failed: %v", err))
	}
	return u.String()
}

// NewWithTimestamp is deliberately absent.
//
// Replay must reuse an event's original identifier rather than minting a fresh one: the
// primary key on processed_events is what makes a replayed event idempotent, so a new
// ID would be a new event and would be applied again (ADR-002, ADR-006). The temptation
// to "re-stamp" events on the way back through the system is exactly the mistake this
// note exists to prevent.

// Parse validates an identifier, returning a typed error rather than a bare bool so the
// caller can tell "not an identifier" from "not this kind of identifier".
func Parse(s string) (uuid.UUID, error) {
	u, err := uuid.Parse(s)
	if err != nil {
		return uuid.Nil, fmt.Errorf("idgen: %q is not a UUID: %w", s, err)
	}
	return u, nil
}

// RandomBytes returns n cryptographically random bytes. Used for the API-key hashing and
// the smoke test's throwaway credentials, so there is one obvious place where randomness
// comes from.
func RandomBytes(n int) ([]byte, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return nil, fmt.Errorf("idgen: read random bytes: %w", err)
	}
	return b, nil
}
