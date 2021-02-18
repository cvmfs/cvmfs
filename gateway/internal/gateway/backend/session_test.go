package backend

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	gw "github.com/cvmfs/gateway/internal/gateway"
)

func TestSessionValid(t *testing.T) {
	lastProtocolVersion := 3
	backend, tmp := StartTestBackend("session_test", 1*time.Second)
	defer func() {
		backend.Stop()
		os.RemoveAll(tmp)
	}()

	ctx := context.TODO()
	keyID := "keyid1"
	leasePath := "test2.repo.org/some/path"
	token, err := backend.NewLease(ctx, keyID, leasePath, lastProtocolVersion)
	if err != nil {
		t.Fatalf("could not obtain new lease: %v", err)
	}
	defer backend.CancelLease(ctx, token)

	payload := strings.NewReader("DUMMY PAYLOAD")
	digest := "abcdef"
	headerSize := 1234

	if err := backend.SubmitPayload(
		ctx, token, payload, digest, headerSize); err != nil {
		t.Fatalf("could not submit payload: %v", err)
	}
	if _, err := backend.CommitLease(ctx, token, "old_hash", "new_hash",
		gw.RepositoryTag{
			Name:        "mytag",
			Channel:     "mychannel",
			Description: "this is a tag",
		}); err != nil {
		t.Fatalf("could not commit existing lease: %v", err)
	}
}

func TestSessionSubmitWithInvalidToken(t *testing.T) {
	lastProtocolVersion := 3
	backend, tmp := StartTestBackend("session_test", 1*time.Second)
	defer func() {
		backend.Stop()
		os.RemoveAll(tmp)
	}()

	ctx := context.TODO()
	keyID := "keyid1"
	leasePath := "test2.repo.org/some/path"
	token, err := backend.NewLease(ctx, keyID, leasePath, lastProtocolVersion)
	if err != nil {
		t.Fatalf("could not obtain new lease: %v", err)
	}
	defer backend.CancelLease(ctx, token)

	token2, err := NewLeaseToken(leasePath, backend.Config.MaxLeaseTime)
	if err != nil {
		t.Fatalf("could not generate second lease token")
	}

	payload := strings.NewReader("DUMMY PAYLOAD")
	digest := "abcdef"
	headerSize := 1234

	if err := backend.SubmitPayload(
		ctx, token2.TokenStr, payload, digest, headerSize); err == nil {
		t.Fatalf("invalid token was not rejected during submission")
	}
}

func TestSessionSubmitWithExpiredToken(t *testing.T) {
	lastProtocolVersion := 3
	backend, tmp := StartTestBackend("session_test", 1*time.Millisecond)
	defer func() {
		backend.Stop()
		os.RemoveAll(tmp)
	}()

	ctx := context.TODO()
	keyID := "keyid1"
	leasePath := "test2.repo.org/some/path"
	token, err := backend.NewLease(ctx, keyID, leasePath, lastProtocolVersion)
	if err != nil {
		t.Fatalf("could not obtain new lease: %v", err)
	}
	defer backend.CancelLease(ctx, token)

	time.Sleep(2 * backend.Config.MaxLeaseTime)

	payload := strings.NewReader("DUMMY PAYLOAD")
	digest := "abcdef"
	headerSize := 1234

	if err := backend.SubmitPayload(
		ctx, token, payload, digest, headerSize); err == nil {
		t.Fatalf("expired token was not rejected during submission")
	}
}

func TestSessionCommitWithInvalidToken(t *testing.T) {
	lastProtocolVersion := 3
	backend, tmp := StartTestBackend("session_test", 1*time.Second)
	defer func() {
		backend.Stop()
		os.RemoveAll(tmp)
	}()

	ctx := context.TODO()
	keyID := "keyid1"
	leasePath := "test2.repo.org/some/path"
	token, err := backend.NewLease(ctx, keyID, leasePath, lastProtocolVersion)
	if err != nil {
		t.Fatalf("could not obtain new lease: %v", err)
	}
	defer backend.CancelLease(ctx, token)

	payload := strings.NewReader("DUMMY PAYLOAD")
	digest := "abcdef"
	headerSize := 1234

	if err := backend.SubmitPayload(
		ctx, token, payload, digest, headerSize); err != nil {
		t.Fatalf("could not submit payload: %v", err)
	}

	token2, err := NewLeaseToken(leasePath, backend.Config.MaxLeaseTime)
	if err != nil {
		t.Fatalf("could not generate second lease token")
	}

	if _, err := backend.CommitLease(ctx, token2.TokenStr, "old_hash", "new_hash",
		gw.RepositoryTag{
			Name:        "mytag",
			Channel:     "mychannel",
			Description: "this is a tag",
		}); err == nil {
		t.Fatalf("invalid token was not rejected during commit action")
	}
}

func TestSessionCommitWithExpiredToken(t *testing.T) {
	lastProtocolVersion := 3
	backend, tmp := StartTestBackend("session_test", 50*time.Millisecond)
	defer func() {
		backend.Stop()
		os.RemoveAll(tmp)
	}()

	ctx := context.TODO()
	keyID := "keyid1"
	leasePath := "test2.repo.org/some/path"
	token, err := backend.NewLease(ctx, keyID, leasePath, lastProtocolVersion)
	if err != nil {
		t.Fatalf("could not obtain new lease: %v", err)
	}
	defer backend.CancelLease(ctx, token)

	payload := strings.NewReader("DUMMY PAYLOAD")
	digest := "abcdef"
	headerSize := 1234

	if err := backend.SubmitPayload(
		ctx, token, payload, digest, headerSize); err != nil {
		t.Fatalf("could not submit payload: %v", err)
	}

	time.Sleep(2 * backend.Config.MaxLeaseTime)

	if _, err := backend.CommitLease(ctx, token, "old_hash", "new_hash",
		gw.RepositoryTag{
			Name:        "mytag",
			Channel:     "mychannel",
			Description: "this is a tag",
		}); err == nil {
		t.Fatalf("expired token was not rejected during commit action")
	}
}

func TestSessionTwoConcurrentValid(t *testing.T) {
	lastProtocolVersion := 3
	backend, tmp := StartTestBackend("session_test", 1*time.Second)
	defer func() {
		backend.Stop()
		os.RemoveAll(tmp)
	}()

	ctx := context.TODO()
	keyID := "keyid1"

	leasePath1 := "test2.repo.org/some/path/one"
	token1, err := backend.NewLease(ctx, keyID, leasePath1, lastProtocolVersion)
	if err != nil {
		t.Fatalf("could not obtain new lease: %v", err)
	}
	defer backend.CancelLease(ctx, token1)

	leasePath2 := "test2.repo.org/some/path/two"
	token2, err := backend.NewLease(ctx, keyID, leasePath2, lastProtocolVersion)
	if err != nil {
		t.Fatalf("could not obtain new lease: %v", err)
	}
	defer backend.CancelLease(ctx, token2)

	payload1 := strings.NewReader("DUMMY PAYLOAD 1")
	digest1 := "abcdef"
	headerSize1 := 1234

	if err := backend.SubmitPayload(
		ctx, token1, payload1, digest1, headerSize1); err != nil {
		t.Fatalf("could not submit payload: %v", err)
	}

	payload2 := strings.NewReader("DUMMY PAYLOAD 2")
	digest2 := "fedcba"
	headerSize2 := 2345

	if err := backend.SubmitPayload(
		ctx, token2, payload2, digest2, headerSize2); err != nil {
		t.Fatalf("could not submit payload: %v", err)
	}

	if _, err := backend.CommitLease(ctx, token1, "old_hash", "new_hash1",
		gw.RepositoryTag{
			Name:        "mytag",
			Channel:     "mychannel",
			Description: "this is a tag",
		}); err != nil {
		t.Fatalf("could not commit existing lease: %v", err)
	}

	if _, err := backend.CommitLease(ctx, token2, "old_hash", "new_hash2",
		gw.RepositoryTag{
			Name:        "mytag",
			Channel:     "mychannel",
			Description: "this is a tag",
		}); err != nil {
		t.Fatalf("could not commit existing lease: %v", err)
	}
}
