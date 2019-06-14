package backend

import (
	"context"
	"time"
)

// GetRepo returns the access configuration of a repository
func (s *Services) GetRepo(ctx context.Context, repoName string) *RepositoryConfig {
	repo := s.Access.GetRepo(repoName)
	if repo != nil {
		repo.Enabled = s.Leases.GetRepositoryEnabled(ctx, repoName)
	}
	return repo
}

// GetRepos returns a map with repository access configurations
func (s *Services) GetRepos(ctx context.Context) map[string]RepositoryConfig {
	repos := s.Access.GetRepos()
	for repoName, cfg := range repos {
		cfg.Enabled = s.Leases.GetRepositoryEnabled(ctx, repoName)
		repos[repoName] = cfg
	}
	return repos
}

// SetRepoEnabled enables or disables a repository. The change does not persist
// across applications restarts
func (s *Services) SetRepoEnabled(ctx context.Context, repository string, enable bool) error {
	t0 := time.Now()

	outcome := "success"
	defer logAction(ctx, "set_repo_enabled", &outcome, t0)

	return s.Leases.SetRepositoryEnabled(ctx, repository, enable)
}

func (s *Services) checkBusy(ctx context.Context, repository string) (bool, error) {
	leases, err := s.GetLeases(ctx)
	if err != nil {
		return false, err
	}
	busy := false
	for n := range leases {
		if n == repository {
			busy = true
			break
		}
	}

	return busy, nil
}
