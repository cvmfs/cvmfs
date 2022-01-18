package backend

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	gw "github.com/cvmfs/gateway/internal/gateway"
)

// ErrRepoDisabled signals that a new lease cannot be acquired due to the repository
// being disabled
var ErrRepoDisabled = fmt.Errorf("repo_disabled")

// SetRepositoryEnabled sets the enabled/disabled status for a given repository
func SetRepositoryEnabled(ctx context.Context, db *DB, repository string, enable bool) error {
	t0 := time.Now()

	txn, err := db.SQL.Begin()
	if err != nil {
		return fmt.Errorf("could not begin transaction: %w", err)
	}
	defer txn.Rollback()

	if enable {
		if _, err := txn.Exec(
			"DELETE FROM DisabledRepo WHERE Name = ?;", repository); err != nil {
			return fmt.Errorf("statement failed: %w", err)
		}
	} else {
		row := db.Statements["getDisabledRepo"].QueryRow(repository)
		var n string
		if err := row.Scan(&n); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				if _, err := txn.Exec(
					"INSERT INTO DisabledRepos (Name) VALUES (?);", repository); err != nil {
					return fmt.Errorf("statement failed: %w", err)
				}
			} else {
				return fmt.Errorf("query failed: %w", err)
			}
		}
	}

	if err := txn.Commit(); err != nil {
		return fmt.Errorf("transaction commit failed: %w", err)
	}

	gw.LogC(ctx, "leasedb", gw.LogDebug).
		Str("operation", "set_repo_enabled").
		Dur("task_dt", time.Since(t0)).
		Str("repository", repository).
		Bool("enabled", enable).
		Msgf("repository status changed")

	return nil
}

// GetRepositoryEnabled returns the enabled status of a repository
func GetRepositoryEnabled(ctx context.Context, db *DB, repository string) bool {
	t0 := time.Now()

	enabled := true

	row := db.Statements["getDisabledRepo"].QueryRow(repository)
	var n string
	if row.Scan(&n) != sql.ErrNoRows {
		enabled = false
	}

	gw.LogC(ctx, "leasedb", gw.LogDebug).
		Str("operation", "get_repo_enabled").
		Dur("task_dt", time.Since(t0)).
		Str("repository", repository).
		Bool("enabled", enabled).
		Msgf("repository status queried")

	return enabled
}
