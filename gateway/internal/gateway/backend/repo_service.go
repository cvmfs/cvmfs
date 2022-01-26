package backend

import (
	"context"
	"fmt"
	"time"
)

func (s *Services) NewRepo(ctx context.Context, name string, enabled bool) error {
	t0 := time.Now()

	outcome := "success"
	defer logAction(ctx, "new_repo", &outcome, t0)

	tx, err := s.DB.SQL.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("could not begin transaction: %w", err)
	}
	defer tx.Rollback()

	repo := Repository{
		Name:     name,
		Manifest: "",
		Enabled:  true,
	}

	if err := CreateRepository(ctx, tx, repo); err != nil {
		return fmt.Errorf("could not create repository: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	return nil
}

// GetRepo returns the access configuration of a repository
func (s *Services) GetRepo(ctx context.Context, repoName string) (*RepositoryConfig, error) {
	t0 := time.Now()

	outcome := "success"
	defer logAction(ctx, "get_repo", &outcome, t0)

	tx, err := s.DB.SQL.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("could not begin transaction: %w", err)
	}
	defer tx.Rollback()

	repo, err := FindRepositoryByName(ctx, tx, repoName)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("could not commit transaction: %w", err)
	}

	repoConfig := s.Access.GetRepo(repoName)
	if repo != nil && repoConfig != nil {
		repoConfig.Enabled = repo.Enabled
	}

	return repoConfig, nil
}

// GetRepos returns a map with repository access configurations
func (s *Services) GetRepos(ctx context.Context) (map[string]RepositoryConfig, error) {
	t0 := time.Now()

	outcome := "success"
	defer logAction(ctx, "get_repos", &outcome, t0)

	tx, err := s.DB.SQL.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("could not begin transaction: %w", err)
	}
	defer tx.Rollback()

	repos, err := FindAllRepositories(ctx, tx)
	if err != nil {
		return nil, err
	}

	repoConfig := s.Access.GetRepos()
	for _, repo := range repos {
		cfg := repoConfig[repo.Name]
		cfg.Enabled = repo.Enabled
		repoConfig[repo.Name] = cfg
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("could not commit transaction: %w", err)
	}

	return repoConfig, nil
}

// SetRepoEnabled enables or disables a repository. The change does not persist
// across applications restarts
func (s *Services) SetRepoEnabled(ctx context.Context, repoName string, enable bool) error {
	t0 := time.Now()

	outcome := "success"
	defer logAction(ctx, "set_repo_enabled", &outcome, t0)

	tx, err := s.DB.SQL.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("could not begin transaction: %w", err)
	}
	defer tx.Rollback()

	repo, err := FindRepositoryByName(ctx, tx, repoName)
	if err != nil {
		return err
	}

	repo.Enabled = enable

	if err := UpdateRepository(ctx, tx, *repo); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	return nil
}

// // SetRepositoryEnabled sets the enabled/disabled status for a given repository
// func SetRepositoryEnabled(ctx context.Context, db *DB, repository string, enable bool) error {
// 	t0 := time.Now()

// 	txn, err := db.SQL.Begin()
// 	if err != nil {
// 		return fmt.Errorf("could not begin transaction: %w", err)
// 	}
// 	defer txn.Rollback()

// 	if enable {
// 		if _, err := txn.Exec(
// 			"DELETE FROM DisabledRepo WHERE Name = ?;", repository); err != nil {
// 			return fmt.Errorf("statement failed: %w", err)
// 		}
// 	} else {
// 		row := db.Statements["getDisabledRepo"].QueryRow(repository)
// 		var n string
// 		if err := row.Scan(&n); err != nil {
// 			if errors.Is(err, sql.ErrNoRows) {
// 				if _, err := txn.Exec(
// 					"INSERT INTO DisabledRepos (Name) VALUES (?);", repository); err != nil {
// 					return fmt.Errorf("statement failed: %w", err)
// 				}
// 			} else {
// 				return fmt.Errorf("query failed: %w", err)
// 			}
// 		}
// 	}

// 	if err := txn.Commit(); err != nil {
// 		return fmt.Errorf("transaction commit failed: %w", err)
// 	}

// 	gw.LogC(ctx, "leasedb", gw.LogDebug).
// 		Str("operation", "set_repo_enabled").
// 		Dur("task_dt", time.Since(t0)).
// 		Str("repository", repository).
// 		Bool("enabled", enable).
// 		Msgf("repository status changed")

// 	return nil
// }

// // GetRepositoryEnabled returns the enabled status of a repository
// func GetRepositoryEnabled(ctx context.Context, db *DB, repository string) bool {
// 	t0 := time.Now()

// 	enabled := true

// 	row := db.Statements["getDisabledRepo"].QueryRow(repository)
// 	var n string
// 	if row.Scan(&n) != sql.ErrNoRows {
// 		enabled = false
// 	}

// 	gw.LogC(ctx, "leasedb", gw.LogDebug).
// 		Str("operation", "get_repo_enabled").
// 		Dur("task_dt", time.Since(t0)).
// 		Str("repository", repository).
// 		Bool("enabled", enabled).
// 		Msgf("repository status queried")

// 	return enabled
// }
