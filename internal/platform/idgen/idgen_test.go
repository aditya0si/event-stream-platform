package idgen_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/aditya0si/event-stream-platform/internal/platform/idgen"
)

func TestNewMintsUUIDv7(t *testing.T) {
	u, err := idgen.Parse(idgen.New())
	if err != nil {
		t.Fatalf("Parse(New()): %v", err)
	}
	if u.Version() != 7 {
		t.Fatalf("version = %d, want 7: event identifiers are the deduplication key, so their "+
			"ordering keeps that index compact under continuous appends", u.Version())
	}
}

func TestIDsMintedLaterSortAfterEarlierOnes(t *testing.T) {
	a := idgen.New()
	// A different millisecond, which is the field the ordering comes from. Sleeping past
	// that field (rather than relying on intra-millisecond counter bits) keeps this test
	// about the property the design depends on: an ID minted later sorts later.
	time.Sleep(3 * time.Millisecond)
	b := idgen.New()

	if !(a < b) {
		t.Fatalf("a later UUIDv7 must sort after an earlier one: %q is not < %q", a, b)
	}
}

func TestParseRejectsNonIdentifiers(t *testing.T) {
	for _, in := range []string{
		"",
		"   ",
		"not-a-uuid",
		"12345678",
		"01a08e6d-bb65-7669-a055", // too short
		"01a08e6d-bb65-7669-a055-d08cb6fe8d82-extra", // too long
		"zzzzzzzz-zzzz-7zzz-zzzz-zzzzzzzzzzzz",       // not hex
	} {
		if _, err := idgen.Parse(in); err == nil {
			t.Errorf("Parse(%q) accepted a value that is not an identifier", in)
		}
	}
}

func TestParseAcceptsWhatNewProduces(t *testing.T) {
	for i := 0; i < 50; i++ {
		if _, err := idgen.Parse(idgen.New()); err != nil {
			t.Fatalf("Parse rejected an identifier New produced: %v", err)
		}
	}
}

func TestRandomBytesAreDistinctAndNonZero(t *testing.T) {
	a, err := idgen.RandomBytes(32)
	if err != nil {
		t.Fatalf("RandomBytes: %v", err)
	}
	if len(a) != 32 {
		t.Fatalf("len = %d, want 32", len(a))
	}

	b, err := idgen.RandomBytes(32)
	if err != nil {
		t.Fatalf("RandomBytes: %v", err)
	}
	if bytes.Equal(a, b) {
		t.Fatal("two calls returned identical bytes: the source is not random")
	}

	var zero [32]byte
	if bytes.Equal(a, zero[:]) {
		t.Fatal("RandomBytes returned all zeroes")
	}
}
