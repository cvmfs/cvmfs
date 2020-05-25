package frontend

import (
	"context"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	gw "github.com/cvmfs/gateway/internal/gateway"
	be "github.com/cvmfs/gateway/internal/gateway/backend"
	"github.com/julienschmidt/httprouter"
)

func forwardBody(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	buf, _ := ioutil.ReadAll(req.Body)
	w.Write(buf)
}

type mockBackend struct {
}

func (b *mockBackend) GetKey(ctx context.Context, keyID string) *be.KeyConfig {
	admin := false
	if strings.HasPrefix(keyID, "admin") {
		admin = true
	}
	return &be.KeyConfig{Secret: "big_secret", Admin: admin}
}

func (b *mockBackend) GetRepo(ctx context.Context, repoName string) *be.RepositoryConfig {
	return &be.RepositoryConfig{
		Keys: be.KeyPaths{"keyid1": "/", "keyid2": "/restricted/to/subdir"},
	}
}

func (b *mockBackend) GetRepos(ctx context.Context) map[string]be.RepositoryConfig {
	return map[string]be.RepositoryConfig{
		"test1.repo.org": be.RepositoryConfig{
			Keys: be.KeyPaths{"keyid123": "/"},
		},
		"test2.repo.org": be.RepositoryConfig{
			Keys: be.KeyPaths{"keyid1": "/", "keyid2": "/restricted/to/subdir"},
		},
	}
}

func (b *mockBackend) SetRepoEnabled(ctx context.Context, repository string, enabled bool) error {
	return nil
}

func (b *mockBackend) NewLease(ctx context.Context, keyID, leasePath string, protocolVersion int) (string, error) {
	return "lease_token_string", nil
}

func (b *mockBackend) GetLeases(ctx context.Context) (map[string]be.LeaseReturn, error) {
	return map[string]be.LeaseReturn{
		"test2.repo.org/some/path/one": be.LeaseReturn{
			KeyID:   "keyid1",
			Expires: time.Now().Add(60 * time.Second).String(),
		},
		"test2.repo.org/some/path/two": be.LeaseReturn{
			KeyID:   "keyid1",
			Expires: time.Now().Add(120 * time.Second).String(),
		},
	}, nil
}

func (b *mockBackend) GetLease(ctx context.Context, tokenStr string) (*be.LeaseReturn, error) {
	return &be.LeaseReturn{
		KeyID:     "keyid1",
		LeasePath: "test2.repo.org/some/path/one",
		Expires:   time.Now().Add(60 * time.Second).String(),
	}, nil
}

func (b *mockBackend) CancelLeases(ctx context.Context, repoPath string) error {
	return nil
}

func (b *mockBackend) CancelLease(ctx context.Context, tokenStr string) error {
	return nil
}

func (b *mockBackend) CommitLease(ctx context.Context, tokenStr, oldRootHash, newRootHash string, tag gw.RepositoryTag) (uint64, error) {
	return 1, nil
}

func (b *mockBackend) SubmitPayload(ctx context.Context, token string, payload io.Reader, digest string, headerSize int) error {
	return nil
}

func (b *mockBackend) RunGC(ctx context.Context, options be.GCOptions) (string, error) {
	return "", nil
}

func (b *mockBackend) PublishManifest(ctx context.Context, repository string, message []byte) error {
	return nil
}

func (b *mockBackend) SubscribeToNotifications(ctx context.Context, repository string) be.SubscriberHandle {
	return make(chan be.NotificationMessage)
}

func (b *mockBackend) UnsubscribeFromNotifications(
	ctx context.Context, repository string, handle be.SubscriberHandle) error {
	return nil
}
