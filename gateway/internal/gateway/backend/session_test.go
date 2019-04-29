package backend

import (
	"context"
	"strings"
	"testing"
	"time"

	gw "github.com/cvmfs/gateway/internal/gateway"
)

func TestValidSession(t *testing.T) {
	backend := StartTestBackend("session_test", 1*time.Second)
	defer backend.Stop()

	ctx := context.TODO()
	keyID := "keyid1"
	leasePath := "test2.repo.org/some/path"
	token, err := NewLease(ctx, backend, keyID, leasePath)
	if err != nil {
		t.Fatalf("could not obtain new lease: %v", err)
	}
	defer CancelLease(ctx, backend, token)

	payload := strings.NewReader("DUMMY PAYLOAD")
	digest := "abcdef"
	headerSize := 1234

	if err := SubmitPayload(
		ctx, backend, token, payload, digest, headerSize); err != nil {
		t.Fatalf("could not submit payload: %v", err)
	}
	if err := CommitLease(ctx, backend, token, "old_hash", "new_hash",
		gw.RepositoryTag{
			Name:        "mytag",
			Channel:     "mychannel",
			Description: "this is a tag",
		}); err != nil {
		t.Fatalf("could not commit existing lease: %v", err)
	}
}

/*
- Start, submit payload with valid token, commit -> OK
- Start, submit payload with invalid token -> ERR
- Start, submit payload with expired token -> ERR
- Start, submit payload with valid token, commit with invalid token -> ERR
- Start, submit payload with valid token, commit with expired token -> ERR
*/
