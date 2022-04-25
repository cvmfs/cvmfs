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

func (s *Services) DeleteAllRepositories(ctx context.Context) error {
	t0 := time.Now()

	outcome := "success"
	defer logAction(ctx, "delete_all", &outcome, t0)

	tx, err := s.DB.SQL.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("could not begin transaction: %w", err)
	}
	defer tx.Rollback()

	if err := DeleteAllRepositories(ctx, tx); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	return nil
}
