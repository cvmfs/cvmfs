package backend

import (
	"testing"
	"time"
)

func TestLeaseToken(t *testing.T) {
	t.Run("identity", func(t *testing.T) {
		token, err := NewLeaseToken("test.repo.org/some/path", 10*time.Second)
		if err != nil {
			t.Fatalf("token generation failed: %v", err)
		}
		repoPath, err := CheckToken(token.TokenStr, token.Secret)
		if err != nil {
			t.Fatalf("valid token was rejected: %v", err)
		}
		if repoPath != "test.repo.org/some/path" {
			t.Fatalf("invalid path was extracted from the token: %v", repoPath)
		}
	})
	t.Run("reject expired", func(t *testing.T) {
		token1, err := NewLeaseToken("test.repo.org/some/path", 1*time.Microsecond)
		if err != nil {
			t.Fatalf("token generation failed: %v", err)
		}
		if _, err := CheckToken(token1.TokenStr, token1.Secret); err == nil {
			t.Fatalf("expired token was accepted")
		} else {
			if _, ok := err.(ExpiredTokenError); !ok {
				t.Fatalf("was expecting ExpiredTokenError")
			}
		}
	})
	t.Run("reject forged", func(t *testing.T) {
		token1, err := NewLeaseToken("test.repo.org/some/path", 10*time.Second)
		if err != nil {
			t.Fatalf("token generation failed: %v", err)
		}
		token2, err := NewLeaseToken("test.repo.org/some/path", 10*time.Second)
		if err != nil {
			t.Fatalf("token generation failed: %v", err)
		}
		if _, err := CheckToken(token2.TokenStr, token1.Secret); err == nil {
			t.Fatalf("forget token was accepted")
		} else {
			if _, ok := err.(InvalidTokenError); !ok {
				t.Fatalf("was expecting InvalidTokenError")
			}
		}
	})
	t.Run("reject garbage", func(t *testing.T) {
		token, err := NewLeaseToken("test.repo.org/some/path", 10*time.Second)
		if err != nil {
			t.Fatalf("token generation failed: %v", err)
		}
		if _, err := CheckToken("gsjhfjdklshfjlksadhflkjsd", token.Secret); err == nil {
			t.Fatalf("garbage string was accepted as real token")
		}
	})
}
