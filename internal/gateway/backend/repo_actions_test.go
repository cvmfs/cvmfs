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

	ctx := context.TODO()
	repos := backend.GetRepos(ctx)
	repoName := "test1.repo.org"
	if !repos[repoName].Enabled {
		t.Fatalf("Repository %v should be enabled by default", repoName)
	}

	backend.SetRepoEnabled(ctx, repoName, false)

	repos = backend.GetRepos(ctx)
	if repos[repoName].Enabled {
		t.Fatalf("Repository %v should have been disabled", repoName)
	}

	backend.SetRepoEnabled(ctx, repoName, true)

	repos = backend.GetRepos(ctx)
	if !repos[repoName].Enabled {
		t.Fatalf("Repository %v should have been reenabled", repoName)
	}
}
