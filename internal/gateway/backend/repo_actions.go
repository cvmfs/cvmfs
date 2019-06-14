package backend

import (
	"context"
	"time"
)

// GetRepo returns the access configuration of a repository
func (s *Services) GetRepo(repoName string) *RepositoryConfig {
	repo := s.Access.GetRepo(repoName)
	if repo != nil {
		repo.Enabled = s.Leases.GetRepositoryEnabled(context.TODO(), repoName)
	}
	return repo
}

// GetRepos returns a map with repository access configurations
func (s *Services) GetRepos() map[string]RepositoryConfig {
	repos := s.Access.GetRepos()
	for repoName, cfg := range repos {
		cfg.Enabled = s.Leases.GetRepositoryEnabled(context.TODO(), repoName)
		repos[repoName] = cfg
	}
	return repos
}

// SetRepoEnabled enables or disables a repository. The change does not persist
// across applications restarts
func (s *Services) SetRepoEnabled(ctx context.Context, repository string, enable bool, wait bool) error {
	t0 := time.Now()

	outcome := "success"
	defer logAction(ctx, "set_repo_enabled", &outcome, t0)

	if !enable {
		// If wait == true, the repository is disabled while the global commit lock
		// of the repository is held
		if wait {
			return s.Leases.WithLock(ctx, repository, func() error {
				busy, err := s.checkBusy(ctx, repository)
				if err != nil {
					outcome = err.Error()
					return err
				}
				if busy {
					outcome = "repository_busy"
					return RepoBusyError{}
				}
				return s.Leases.SetRepositoryEnabled(ctx, repository, false)
			})
		}

		busy, err := s.checkBusy(ctx, repository)
		if err != nil {
			outcome = err.Error()
			return err
		}
		if busy {
			outcome = "repository_busy"
			return RepoBusyError{}
		}
	}

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
