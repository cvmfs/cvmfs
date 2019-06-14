package backend

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestRepoActionsToggleRepo(t *testing.T) {
	backend, tmp := StartTestBackend("repo_actions_toggle_test", 1*time.Second)
	defer func() {
		backend.Stop()
		os.RemoveAll(tmp)
	}()

	repos := backend.GetRepos()
	repoName := "test1.repo.org"
	if !repos[repoName].Enabled {
		t.Fatalf("Repository %v should be enabled by default", repoName)
	}

	backend.SetRepoEnabled(context.TODO(), repoName, false, false)

	repos = backend.GetRepos()
	if repos[repoName].Enabled {
		t.Fatalf("Repository %v should have been disabled", repoName)
	}
}
