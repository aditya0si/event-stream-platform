package testsupport

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// dlqLockKey is a fixed, arbitrary advisory-lock key. Only its consistency matters: every test
// that asserts on the dead-letter queue as a whole uses the same number.
const dlqLockKey int64 = 0x65737064 // "espd"

// LockDLQ serializes tests that assert on the dead-letter queue as a whole.
//
// The queue is shared state by nature — it is one table — and `go test ./...` runs packages in
// parallel against one database. A test that reads the depth, inserts a row, and reads the
// depth again is therefore asserting on a number that another package can move underneath it,
// and it fails for a reason that has nothing to do with the code under test. That is exactly
// what happened when the replay package's tests were added: the sink package's depth assertion
// had been passing and started failing, while both packages were individually correct.
//
// A Postgres advisory lock rather than a Go mutex, because the contending tests are different
// processes. It is released when the test ends, and also if the test binary dies — the server
// drops a session's locks when the connection goes.
//
// Tests that only *write* dead letters must take it too, or they will break the assertions of
// whichever package is holding it.
func LockDLQ(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	ctx := context.Background()

	// The connection is held for the whole test rather than returned to the pool: an advisory
	// lock belongs to a session, and handing the connection back would leave the lock attached
	// to something the test no longer controls.
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("testsupport: acquire a connection for the dead-letter lock: %v", err)
	}
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", dlqLockKey); err != nil {
		conn.Release()
		t.Fatalf("testsupport: take the dead-letter lock: %v", err)
	}

	t.Cleanup(func() {
		// A background context deliberately: a test's own context is frequently cancelled by
		// the time cleanup runs, and an unlock that failed for that reason would hold the lock
		// until the connection was reaped, stalling every later test that needs it.
		if _, err := conn.Exec(context.Background(),
			"SELECT pg_advisory_unlock($1)", dlqLockKey); err != nil {
			t.Logf("testsupport: releasing the dead-letter lock failed; later tests may block: %v", err)
		}
		conn.Release()
	})
}
