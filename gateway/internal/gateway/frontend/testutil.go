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

func (b *mockBackend) GetKey(keyID string) *be.KeyConfig {
	admin := false
	if strings.HasPrefix(keyID, "admin") {
		admin = true
	}
	return &be.KeyConfig{Secret: "big_secret", Admin: admin}
}

func (b *mockBackend) GetRepo(repoName string) *be.RepositoryConfig {
	return &be.RepositoryConfig{
		Keys:    be.KeyPaths{"keyid1": "/", "keyid2": "/restricted/to/subdir"},
		Enabled: true,
	}
}

func (b *mockBackend) GetRepos() map[string]be.RepositoryConfig {
	return map[string]be.RepositoryConfig{
		"test1.repo.org": be.RepositoryConfig{
			Keys:    be.KeyPaths{"keyid123": "/"},
			Enabled: true,
		},
		"test2.repo.org": be.RepositoryConfig{
			Keys:    be.KeyPaths{"keyid1": "/", "keyid2": "/restricted/to/subdir"},
			Enabled: true,
		},
	}
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

func (b *mockBackend) CancelLease(ctx context.Context, tokenStr string) error {
	return nil
}

func (b *mockBackend) CommitLease(ctx context.Context, tokenStr, oldRootHash, newRootHash string, tag gw.RepositoryTag) error {
	return nil
}

func (b *mockBackend) SubmitPayload(ctx context.Context, token string, payload io.Reader, digest string, headerSize int) error {
	return nil
}
