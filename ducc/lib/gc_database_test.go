package lib

import (
	"os"
	"path/filepath"
	"testing"
)

func tempDBPath(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	return filepath.Join(dir, "test_gc.db")
}

func TestOpenGCDatabase_CreatesNewDB(t *testing.T) {
	dbPath := tempDBPath(t)
	db, err := OpenGCDatabase(dbPath)
	if err != nil {
		t.Fatalf("OpenGCDatabase failed: %v", err)
	}
	defer db.Close()

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Error("database file was not created")
	}
}

func TestOpenGCDatabase_ReopensExisting(t *testing.T) {
	dbPath := tempDBPath(t)
	db, err := OpenGCDatabase(dbPath)
	if err != nil {
		t.Fatalf("first open failed: %v", err)
	}
	if err := db.InsertPaths([]string{"a/b"}, "layer"); err != nil {
		t.Fatalf("insert failed: %v", err)
	}
	db.Close()

	db2, err := OpenGCDatabase(dbPath)
	if err != nil {
		t.Fatalf("second open failed: %v", err)
	}
	defer db2.Close()

	paths, err := db2.PendingPaths()
	if err != nil {
		t.Fatalf("PendingPaths failed: %v", err)
	}
	if len(paths) != 1 || paths[0] != "a/b" {
		t.Errorf("expected [a/b], got %v", paths)
	}
}

func TestInsertPaths_BasicInsert(t *testing.T) {
	db, err := OpenGCDatabase(tempDBPath(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.InsertPaths([]string{"img/1", "img/2", "img/3"}, "image")
	if err != nil {
		t.Fatalf("InsertPaths failed: %v", err)
	}

	paths, err := db.PendingPaths()
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 3 {
		t.Errorf("expected 3 paths, got %d", len(paths))
	}
}

func TestInsertPaths_DuplicatesIgnored(t *testing.T) {
	db, err := OpenGCDatabase(tempDBPath(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.InsertPaths([]string{"a/b", "c/d"}, "layer")
	db.InsertPaths([]string{"a/b", "e/f"}, "layer") // a/b is duplicate

	paths, _ := db.PendingPaths()
	if len(paths) != 3 {
		t.Errorf("expected 3 unique paths, got %d: %v", len(paths), paths)
	}
}

func TestInsertPaths_EmptySlice(t *testing.T) {
	db, err := OpenGCDatabase(tempDBPath(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.InsertPaths([]string{}, "image")
	if err != nil {
		t.Fatalf("InsertPaths with empty slice failed: %v", err)
	}

	paths, _ := db.PendingPaths()
	if len(paths) != 0 {
		t.Errorf("expected 0 paths, got %d", len(paths))
	}
}

func TestInsertPaths_MultipleCategories(t *testing.T) {
	db, err := OpenGCDatabase(tempDBPath(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.InsertPaths([]string{"img/1"}, "image")
	db.InsertPaths([]string{"layer/1"}, "layer")
	db.InsertPaths([]string{"podman/1"}, "podman")

	paths, _ := db.PendingPaths()
	if len(paths) != 3 {
		t.Errorf("expected 3 paths across categories, got %d", len(paths))
	}
}

func TestPendingPaths_OrderedById(t *testing.T) {
	db, err := OpenGCDatabase(tempDBPath(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.InsertPaths([]string{"z/first", "a/second", "m/third"}, "layer")

	paths, _ := db.PendingPaths()
	expected := []string{"z/first", "a/second", "m/third"}
	for i, p := range paths {
		if p != expected[i] {
			t.Errorf("path[%d]: expected %s, got %s", i, expected[i], p)
		}
	}
}

func TestMarkDeleted_BasicFlow(t *testing.T) {
	db, err := OpenGCDatabase(tempDBPath(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.InsertPaths([]string{"a", "b", "c"}, "layer")

	err = db.MarkDeleted([]string{"a", "c"})
	if err != nil {
		t.Fatalf("MarkDeleted failed: %v", err)
	}

	paths, _ := db.PendingPaths()
	if len(paths) != 1 || paths[0] != "b" {
		t.Errorf("expected only [b] pending, got %v", paths)
	}
}

func TestMarkDeleted_AllPaths(t *testing.T) {
	db, err := OpenGCDatabase(tempDBPath(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.InsertPaths([]string{"x", "y"}, "image")
	db.MarkDeleted([]string{"x", "y"})

	paths, _ := db.PendingPaths()
	if len(paths) != 0 {
		t.Errorf("expected 0 pending, got %d", len(paths))
	}
}

func TestMarkDeleted_NonexistentPathIsNoop(t *testing.T) {
	db, err := OpenGCDatabase(tempDBPath(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.InsertPaths([]string{"a"}, "layer")
	err = db.MarkDeleted([]string{"nonexistent"})
	if err != nil {
		t.Fatalf("MarkDeleted on nonexistent path should not error: %v", err)
	}

	paths, _ := db.PendingPaths()
	if len(paths) != 1 {
		t.Errorf("expected 1 pending, got %d", len(paths))
	}
}

func TestSummary_Counts(t *testing.T) {
	db, err := OpenGCDatabase(tempDBPath(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.InsertPaths([]string{"a", "b", "c", "d", "e"}, "layer")
	db.MarkDeleted([]string{"a", "c"})

	pending, deleted, err := db.Summary()
	if err != nil {
		t.Fatal(err)
	}
	if pending != 3 {
		t.Errorf("expected 3 pending, got %d", pending)
	}
	if deleted != 2 {
		t.Errorf("expected 2 deleted, got %d", deleted)
	}
}

func TestSummary_EmptyDB(t *testing.T) {
	db, err := OpenGCDatabase(tempDBPath(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	pending, deleted, err := db.Summary()
	if err != nil {
		t.Fatal(err)
	}
	if pending != 0 || deleted != 0 {
		t.Errorf("expected 0/0, got %d/%d", pending, deleted)
	}
}

func TestSaveRepoMetadata_AndRetrieve(t *testing.T) {
	db, err := OpenGCDatabase(tempDBPath(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	meta := RepoMetadata{
		Name:            "test.cern.ch",
		Revision:        "42",
		RootCatalogHash: "abc123def456",
	}
	err = db.SaveRepoMetadata(meta)
	if err != nil {
		t.Fatalf("SaveRepoMetadata failed: %v", err)
	}

	got, err := db.GetRepoMetadataFromDB()
	if err != nil {
		t.Fatalf("GetRepoMetadataFromDB failed: %v", err)
	}
	if got.Name != meta.Name {
		t.Errorf("name: expected %s, got %s", meta.Name, got.Name)
	}
	if got.Revision != meta.Revision {
		t.Errorf("revision: expected %s, got %s", meta.Revision, got.Revision)
	}
	if got.RootCatalogHash != meta.RootCatalogHash {
		t.Errorf("hash: expected %s, got %s", meta.RootCatalogHash, got.RootCatalogHash)
	}
}

func TestSaveRepoMetadata_OverwritesPrevious(t *testing.T) {
	db, err := OpenGCDatabase(tempDBPath(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.SaveRepoMetadata(RepoMetadata{Name: "old.cern.ch", Revision: "1", RootCatalogHash: "aaa"})
	db.SaveRepoMetadata(RepoMetadata{Name: "new.cern.ch", Revision: "2", RootCatalogHash: "bbb"})

	got, err := db.GetRepoMetadataFromDB()
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "new.cern.ch" {
		t.Errorf("expected new.cern.ch, got %s", got.Name)
	}
	if got.Revision != "2" {
		t.Errorf("expected revision 2, got %s", got.Revision)
	}
}

func TestGetRepoMetadataFromDB_EmptyReturnsNotExist(t *testing.T) {
	db, err := OpenGCDatabase(tempDBPath(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.GetRepoMetadataFromDB()
	if !os.IsNotExist(err) {
		t.Errorf("expected os.ErrNotExist, got %v", err)
	}
}

func TestLargeInsert(t *testing.T) {
	db, err := OpenGCDatabase(tempDBPath(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	paths := make([]string, 10000)
	for i := range paths {
		paths[i] = filepath.Join(".layers", "sha256", "abcdef"+string(rune('0'+i%10)), "layer")
	}

	err = db.InsertPaths(paths, "layer")
	if err != nil {
		t.Fatalf("large InsertPaths failed: %v", err)
	}

	pending, _, _ := db.Summary()
	// duplicates from the modular naming: only 10 unique paths
	if pending != 10 {
		t.Errorf("expected 10 unique paths, got %d", pending)
	}
}

func TestFullWorkflow_ScanThenDelete(t *testing.T) {
	dbPath := tempDBPath(t)

	// Phase 1: "scan" — open DB, insert paths, save metadata, close
	db, err := OpenGCDatabase(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	db.SaveRepoMetadata(RepoMetadata{
		Name: "unpacked.cern.ch", Revision: "100", RootCatalogHash: "deadbeef",
	})
	db.InsertPaths([]string{".flat/img1", ".flat/img2"}, "image")
	db.InsertPaths([]string{".layers/l1", ".layers/l2", ".layers/l3"}, "layer")
	db.Close()

	// Phase 2: "delete" — reopen DB, read pending, mark deleted, close
	db2, err := OpenGCDatabase(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	meta, err := db2.GetRepoMetadataFromDB()
	if err != nil {
		t.Fatal(err)
	}
	if meta.Name != "unpacked.cern.ch" || meta.Revision != "100" {
		t.Errorf("metadata mismatch: %+v", meta)
	}

	paths, err := db2.PendingPaths()
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 5 {
		t.Fatalf("expected 5 pending paths, got %d", len(paths))
	}

	// Simulate successful deletion
	err = db2.MarkDeleted(paths)
	if err != nil {
		t.Fatal(err)
	}

	pending, deleted, _ := db2.Summary()
	if pending != 0 {
		t.Errorf("expected 0 pending, got %d", pending)
	}
	if deleted != 5 {
		t.Errorf("expected 5 deleted, got %d", deleted)
	}
}
