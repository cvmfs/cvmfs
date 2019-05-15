package backend

import (
	"strings"
	"testing"
)

func TestEmptyAccessConfig(t *testing.T) {
	t.Run("empty no version", func(t *testing.T) {
		ac := emptyAccessConfig()
		rd := strings.NewReader("{}")
		err := ac.load(rd, mockKeyImporter)
		if err != nil {
			t.Fatalf("access config loading failed: %v", err)
		}
	})
	t.Run("empty version 1", func(t *testing.T) {
		ac := emptyAccessConfig()
		rd := strings.NewReader("{\"version\": 1}")
		err := ac.load(rd, mockKeyImporter)
		if err != nil {
			t.Fatalf("access config loading failed: %v", err)
		}
	})
	t.Run("empty version 2", func(t *testing.T) {
		ac := emptyAccessConfig()
		rd := strings.NewReader("{\"version\": 2}")
		err := ac.load(rd, mockKeyImporter)
		if err != nil {
			t.Fatalf("access config loading failed: %v", err)
		}
	})
}

func TestLoadAccessConfigVersion1(t *testing.T) {
	ac := emptyAccessConfig()
	rd := strings.NewReader(accessConfigV1)
	err := ac.load(rd, mockKeyImporter)
	if err != nil {
		t.Fatalf("access config loading failed: %v", err)
	}
	if len(ac.Repositories) != 1 && len(ac.Keys) != 2 {
		t.Fatalf("invalid access config (missing items): %+v", ac)
	}
}

func TestLoadAccessConfigVersion2(t *testing.T) {
	ac := emptyAccessConfig()
	rd := strings.NewReader(accessConfigV2)
	err := ac.load(rd, mockKeyImporter)
	if err != nil {
		t.Fatalf("access config loading failed: %v", err)
	}
	if len(ac.Repositories) != 2 && len(ac.Keys) != 2 {
		t.Fatalf("invalid access config (missing items): %+v", ac)
	}
	if _, present := (ac.Repositories["test1.repo.org"].Keys["keyid123"]); !present {
		t.Fatalf("invalid access config (placeholder was not substituted): %+v", ac)
	}
	if _, present := ac.Keys["keyid123"]; !present {
		t.Fatalf("invalid access config (default key was not loaded): %+v", ac)
	}
}

func TestLoadAccessConfigVersion2NoKeys(t *testing.T) {
	ac := emptyAccessConfig()
	rd := strings.NewReader(accessConfigV2NoKeys)
	err := ac.load(rd, mockKeyImporter)
	if err != nil {
		t.Fatalf("access config loading failed: %v", err)
	}
	if len(ac.Repositories) != 1 && len(ac.Keys) != 1 {
		t.Fatalf("invalid access config (missing items): %+v", ac)
	}
	if _, present := (ac.Repositories["test1.repo.org"].Keys["keyid123"]); !present {
		t.Fatalf("invalid access config (placeholder was not substituted): %+v", ac)
	}
	if _, present := ac.Keys["keyid123"]; !present {
		t.Fatalf("invalid access config (default key was not loaded): %+v", ac)
	}
}

func TestKeyCheck(t *testing.T) {
	ac := emptyAccessConfig()
	rd := strings.NewReader(accessConfigV2)
	err := ac.load(rd, mockKeyImporter)
	if err != nil {
		t.Fatalf("access config loading failed: %v", err)
	}
	t.Run("valid", func(t *testing.T) {
		if err := ac.Check("keyid2", "/restricted/to/subdir/below", "test2.repo.org"); err != nil {
			t.Errorf("valid key was rejected: %v", err)
		}
	})
	t.Run("invalid_repo", func(t *testing.T) {
		if ac.Check("keyid2", "/restricted/to/subdir/below", "invalid.repo.org") == nil {
			t.Errorf("invalid repo was accepted")
		}
	})
	t.Run("invalid_key", func(t *testing.T) {
		if ac.Check("badkey", "/restricted/to/subdir/below", "test2.repo.org") == nil {
			t.Errorf("invalid key was accepted")
		}
	})
	t.Run("invalid_path", func(t *testing.T) {
		if ac.Check("keyid2", "/invalid/path", "test2.repo.org") == nil {
			t.Errorf("invalid path was accepted")
		}
	})
}
