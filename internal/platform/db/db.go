// Package db owns the Postgres connection pool and the transaction helper the sink and
// the deduplication store are built on.
//
// The transaction helper is the important part. ADR-002 and ADR-006 both rest on one
// claim: the deduplication record and every other effect of an event commit together or
// not at all. That claim is only true if call sites cannot accidentally write outside a
// transaction, so this package makes the transactional path the easy one.
package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Options configures the pool. It mirrors the validated config rather than taking the
// whole config struct, so this package does not depend on the config package.
type Options struct {
	URL             string
	MaxConns        int32
	MinConns        int32
	MaxConnLifetime time.Duration
	ConnectTimeout  time.Duration
}

// Open creates a pool and proves it can reach the database before returning.
//
// The ping is not ceremony: a process that starts with an unreachable database and only
// discovers it on the first request is a process that reports healthy while being
// useless. Failing here means the container exits and the orchestrator's restart policy
// has something real to react to.
func Open(ctx context.Context, opts Options) (*pgxpool.Pool, error) {
	pc, err := pgxpool.ParseConfig(opts.URL)
	if err != nil {
		// The error from pgx includes the DSN, which may carry a password. Do not wrap it
		// verbatim into anything that could be logged.
		return nil, errors.New("db: DATABASE_URL is not a valid Postgres connection string")
	}

	pc.MaxConns = opts.MaxConns
	pc.MinConns = opts.MinConns
	pc.MaxConnLifetime = opts.MaxConnLifetime

	// A per-connection connect timeout, so a black-holed address fails in seconds rather
	// than at the OS TCP timeout (which can be minutes).
	if opts.ConnectTimeout > 0 {
		pc.ConnConfig.ConnectTimeout = opts.ConnectTimeout
	}

	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("db: create pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, opts.ConnectTimeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("db: ping failed: %w", err)
	}
	return pool, nil
}

// InTx runs fn inside a transaction, committing on a nil return and rolling back on any
// error — including a panic, which is re-raised after the rollback.
//
// Every write in this system goes through here. That is the mechanism behind the
// effectively-once claim (ADR-002): the dedup insert, the history append, the current
// state upsert, and the schema-version counter share one fate, so there is no window in
// which an event is half-applied.
func InTx(ctx context.Context, pool *pgxpool.Pool, fn func(pgx.Tx) error) error {
	if pool == nil {
		return errors.New("db: nil pool")
	}
	// pgx.BeginFunc rolls back on any error returned from fn, and — importantly — does not
	// roll back on a nil return. The consumer relies on the latter to commit offsets only
	// after this returns nil.
	return pgx.BeginFunc(ctx, pool, fn)
}
