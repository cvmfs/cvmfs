package backend

import (
	"context"
	"time"
)

// GetRepo returns the access configuration of a repository
func (s *Services) GetRepo(repoName string) *RepositoryConfig {
	return s.Access.GetRepo(repoName)
}

// GetRepos returns a map with repository access configurations
func (s *Services) GetRepos() map[string]RepositoryConfig {
	return s.Access.GetRepos()
}

// SetRepoEnabled enables or disables a repository. The change does not persist
// across applications restarts
func (s *Services) SetRepoEnabled(ctx context.Context, repository string, enable bool, wait bool) error {
	t0 := time.Now()

	outcome := "success"
	defer logAction(ctx, "new_lease", &outcome, t0)

	if !enable {
		// If wait == true, the repository is disabled while the global commit lock
		// of the repository is held
		if wait {
			s.Leases.WithLock(ctx, repository, func() error {
				s.Access.SetRepositoryEnabled(repository, enable)
				return nil
			})
			return nil
		}

		leases, err := s.GetLeases(ctx)
		if err != nil {
			outcome = err.Error()
			return err
		}
		busy := false
		for n := range leases {
			if n == repository {
				busy = true
				break
			}
		}
		if busy {
			outcome = "repository_busy"
			return RepoBusyError{}
		}
	}

	s.Access.SetRepositoryEnabled(repository, enable)

	return nil
}
