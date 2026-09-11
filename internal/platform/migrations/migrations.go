// Package migrations applies forward-only SQL migrations embedded in the binary.
//
// Forward-only is deliberate, and the reasoning is the sibling repository's ADR-011: a
// down-migration is a second implementation of the schema, written under time pressure
// during an incident, that is almost never exercised until the moment it must work. The
// realistic recovery path for a bad migration is to write the next one.
//
// Applied migrations are recorded in schema_migrations, so `up` is idempotent and
// `status` can answer "is this deployment behind?" without guessing. Each migration runs
// inside a transaction together with its own bookkeeping insert: a migration either
// applies completely and is recorded, or it does not apply at all.
package migrations

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed *.sql
var files embed.FS

// Applied describes one migration's state.
type Applied struct {
	Version   string
	Name      string
	Bytes     int
	AppliedAt time.Time
}

// Pending describes a migration that has not been applied.
type Pending struct {
	Version string
	Name    string
	Bytes   int
}

// Result is the outcome of an `up` run.
type Result struct {
	Applied []Applied
	Pending []Pending
}

type migration struct {
	version string
	name    string
	body    string
}

// load reads and orders the embedded migrations.
//
// Ordering is lexical on the filename prefix, which is why the files are named with a
// zero-padded number: 0001_, 0002_, and so on. Lexical order on zero-padded names is
// chronological order, and no separate index file can drift out of sync with the
// directory.
func load() ([]migration, error) {
	entries, err := fs.ReadDir(files, ".")
	if err != nil {
		return nil, fmt.Errorf("migrations: read embedded dir: %w", err)
	}
	var out []migration
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".sql") {
			continue
		}
		body, err := fs.ReadFile(files, name)
		if err != nil {
			return nil, fmt.Errorf("migrations: read %s: %w", name, err)
		}
		// The version is everything before the first underscore; the name is the rest.
		// A file that does not follow the convention is a mistake worth failing on, not
		// skipping — a silently ignored migration file is a schema nobody applied.
		idx := strings.Index(name, "_")
		if idx <= 0 {
			return nil, fmt.Errorf("migrations: %q does not follow NNNN_description.sql", name)
		}
		out = append(out, migration{
			version: name[:idx],
			name:    strings.TrimSuffix(name[idx+1:], ".sql"),
			body:    string(body),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	if len(out) == 0 {
		return nil, errors.New("migrations: none embedded (the //go:embed pattern matched nothing)")
	}
	return out, nil
}

// ensureTable creates the bookkeeping table if it is missing.
func ensureTable(ctx context.Context, pool *pgxpool.Pool) error {
	const ddl = `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    text        PRIMARY KEY,
			name       text        NOT NULL,
			checksum   text        NOT NULL,
			applied_at timestamptz NOT NULL DEFAULT now()
		)`
	if _, err := pool.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("migrations: create schema_migrations: %w", err)
	}
	return nil
}

// Status reports what is applied and what is pending, without changing anything.
func Status(ctx context.Context, pool *pgxpool.Pool) (Result, error) {
	if err := ensureTable(ctx, pool); err != nil {
		return Result{}, err
	}
	all, err := load()
	if err != nil {
		return Result{}, err
	}

	rows, err := pool.Query(ctx, `SELECT version, name, applied_at FROM schema_migrations ORDER BY version`)
	if err != nil {
		return Result{}, fmt.Errorf("migrations: query applied: %w", err)
	}
	defer rows.Close()

	applied := map[string]Applied{}
	for rows.Next() {
		var a Applied
		if err := rows.Scan(&a.Version, &a.Name, &a.AppliedAt); err != nil {
			return Result{}, fmt.Errorf("migrations: scan applied: %w", err)
		}
		applied[a.Version] = a
	}
	if err := rows.Err(); err != nil {
		return Result{}, fmt.Errorf("migrations: iterate applied: %w", err)
	}

	var res Result
	for _, m := range all {
		if a, ok := applied[m.version]; ok {
			a.Bytes = len(m.body)
			res.Applied = append(res.Applied, a)
			continue
		}
		res.Pending = append(res.Pending, Pending{Version: m.version, Name: m.name, Bytes: len(m.body)})
	}
	return res, nil
}

// Up applies every pending migration in order, stopping at the first failure.
//
// Stopping is the correct behaviour: later migrations may depend on earlier ones, and
// continuing past a failure would build a schema on a foundation that is not there.
// Because each migration is transactional, a failure leaves the database at the last
// completely-applied version — never half-way through one.
func Up(ctx context.Context, pool *pgxpool.Pool) (Result, error) {
	if err := ensureTable(ctx, pool); err != nil {
		return Result{}, err
	}
	all, err := load()
	if err != nil {
		return Result{}, err
	}

	current, err := Status(ctx, pool)
	if err != nil {
		return Result{}, err
	}
	done := map[string]bool{}
	for _, a := range current.Applied {
		done[a.Version] = true
	}

	var res Result
	for _, m := range all {
		if done[m.version] {
			continue
		}
		err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, m.body); err != nil {
				return fmt.Errorf("apply: %w", err)
			}
			if _, err := tx.Exec(ctx,
				`INSERT INTO schema_migrations (version, name, checksum) VALUES ($1, $2, '')`,
				m.version, m.name); err != nil {
				return fmt.Errorf("record: %w", err)
			}
			return nil
		})
		if err != nil {
			return res, fmt.Errorf("migrations: %s_%s failed: %w", m.version, m.name, err)
		}
		res.Applied = append(res.Applied, Applied{
			Version: m.version, Name: m.name, Bytes: len(m.body), AppliedAt: time.Now().UTC(),
		})
	}
	return res, nil
}
