package backend

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	gw "github.com/cvmfs/gateway/internal/gateway"
)

// ErrRepoDisabled signals that a new lease cannot be acquired due to the repository
// being disabled
var ErrRepoDisabled = fmt.Errorf("repo_disabled")

type Repository struct {
	Name     string
	Manifest string
	Enabled  bool
}

func CreateRepository(ctx context.Context, tx *sql.Tx, repo Repository) error {
	t0 := time.Now()

	res, err := tx.ExecContext(ctx,
		"insert into Repository (Name, Manifest, Enabled) values (?, ?, ?);",
		repo.Name, repo.Manifest, repo.Enabled)
	if err != nil {
		return fmt.Errorf("could not insert new repository: %w", err)
	}
	numInserts, err := res.RowsAffected()
	// err should be nil if DB driver returns the number of affected rows
	if err == nil && numInserts == 0 {
		return fmt.Errorf("new repository not inserted")
	}

	gw.LogC(ctx, "repository_entity", gw.LogDebug).
		Str("operation", "create").
		Dur("task_dt", time.Since(t0)).
		Msgf("name: %v, enabled: %v", repo.Name, repo.Enabled)

	return nil
}

func UpdateRepository(ctx context.Context, tx *sql.Tx, repo Repository) error {
	t0 := time.Now()

	res, err := tx.ExecContext(ctx,
		"update Repository set Manifest = ?, Enabled = ? where Name = ?;",
		repo.Manifest, repo.Enabled, repo.Name)
	if err != nil {
		return fmt.Errorf("could not update repository: %w", err)
	}
	numUpdates, err := res.RowsAffected()
	// err should be nil if DB driver returns the number of affected rows
	if err == nil && numUpdates != 1 {
		return fmt.Errorf("repository not updated")
	}

	gw.LogC(ctx, "repository_entity", gw.LogDebug).
		Str("operation", "update").
		Dur("task_dt", time.Since(t0)).
		Msgf("name: %v, enabled: %v", repo.Name, repo.Enabled)

	return nil
}

func FindAllRepositories(ctx context.Context, tx *sql.Tx) ([]Repository, error) {
	t0 := time.Now()

	rows, err := tx.QueryContext(
		ctx,
		"select * from Repository;")
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	repositories := make([]Repository, 0)
	for rows.Next() {
		var repo Repository
		if err := scanRepository(rows, &repo); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		repositories = append(repositories, repo)
	}

	gw.LogC(ctx, "repository_entity", gw.LogDebug).
		Str("operation", "find_all").
		Dur("task_dt", time.Since(t0)).
		Msgf("num repositories: %v", len(repositories))

	return repositories, nil
}

func FindRepositoryByName(ctx context.Context, tx *sql.Tx, name string) (*Repository, error) {
	t0 := time.Now()

	rows, err := tx.QueryContext(
		ctx,
		"select * from Repository where Name = ?;", name)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var repo Repository
	if rows.Next() {
		if err := scanRepository(rows, &repo); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
	} else {
		return nil, nil
	}

	gw.LogC(ctx, "repository_entity", gw.LogDebug).
		Str("operation", "find_by_name").
		Dur("task_dt", time.Since(t0)).
		Msgf("success")

	return &repo, nil
}

func DeleteAllRepositories(ctx context.Context, tx *sql.Tx) error {
	t0 := time.Now()

	_, err := tx.ExecContext(ctx, "delete from Repository;")
	if err != nil {
		return fmt.Errorf("could not update repository: %w", err)
	}

	gw.LogC(ctx, "repository_entity", gw.LogDebug).
		Str("operation", "delete_all").
		Dur("task_dt", time.Since(t0)).
		Msgf("success")

	return nil
}

func scanRepository(rows *sql.Rows, repo *Repository) error {
	if err := rows.Scan(
		&repo.Name,
		&repo.Manifest,
		&repo.Enabled); err != nil {
		return err
	}

	return nil
}
