package lib

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"
)

// GCDatabase wraps an SQLite database that stores paths marked for deletion
// during garbage collection. It allows the scan and delete phases to be
// performed independently.
type GCDatabase struct {
	db *sql.DB
}

// RepoMetadata holds the CVMFS repository metadata obtained from extended
// attributes on the repository root directory.
type RepoMetadata struct {
	Name            string // fully qualified repository name (fqrn)
	Revision        string // current revision number
	RootCatalogHash string // hash of the root catalog
}

// GetRepoMetadata reads the repository name, revision, and root catalog hash
// from the extended attributes of the mounted CVMFS repository.
func GetRepoMetadata(CVMFSRepo string) (RepoMetadata, error) {
	root := filepath.Join("/", "var", "spool", "cvmfs", CVMFSRepo, "rdonly")

	readXattr := func(attr string) (string, error) {
		buf := make([]byte, 256)
		size, err := unix.Getxattr(root, "user."+attr, buf)
		if err != nil {
			return "", fmt.Errorf("failed to read xattr user.%s on %s: %w", attr, root, err)
		}
		return strings.TrimSpace(string(buf[:size])), nil
	}

	fqrn, err := readXattr("fqrn")
	if err != nil {
		// Fall back to deriving the name from the path / argument
		fqrn = filepath.Base(CVMFSRepo)
	}
	revision, err := readXattr("revision")
	if err != nil {
		revision = ""
	}
	rootHash, err := readXattr("root_hash")
	if err != nil {
		rootHash = ""
	}

	return RepoMetadata{
		Name:            fqrn,
		Revision:        revision,
		RootCatalogHash: rootHash,
	}, nil
}

// OpenGCDatabase opens (or creates) the SQLite database at dbPath and ensures
// the schema exists.
func OpenGCDatabase(dbPath string) (*GCDatabase, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open gc database %s: %w", dbPath, err)
	}

	// WAL mode for better concurrent access and performance
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to set WAL mode: %w", err)
	}

	schema := `
	CREATE TABLE IF NOT EXISTS gc_paths (
		id        INTEGER PRIMARY KEY AUTOINCREMENT,
		path      TEXT    NOT NULL UNIQUE,
		category  TEXT    NOT NULL,  -- 'image', 'layer', or 'podman'
		scanned_at TEXT   NOT NULL,  -- RFC3339 timestamp of when the path was recorded
		deleted   INTEGER NOT NULL DEFAULT 0  -- 0 = pending, 1 = deleted
	);
	CREATE INDEX IF NOT EXISTS idx_gc_paths_deleted ON gc_paths(deleted);
	CREATE INDEX IF NOT EXISTS idx_gc_paths_category ON gc_paths(category);

	CREATE TABLE IF NOT EXISTS repo_metadata (
		id                 INTEGER PRIMARY KEY CHECK (id = 1),  -- exactly one row
		repo_name          TEXT    NOT NULL,
		revision           TEXT    NOT NULL,
		root_catalog_hash  TEXT    NOT NULL,
		scanned_at         TEXT    NOT NULL
	);
	`
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create gc database schema: %w", err)
	}

	// Check for the existence of repo_metadata table in older databases
	// that were created before this column was added.
	var tableName string
	err = db.QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name='repo_metadata'`).Scan(&tableName)
	if err != nil {
		// Table doesn't exist in an older database — create it now
		_, err = db.Exec(`CREATE TABLE IF NOT EXISTS repo_metadata (
			id                 INTEGER PRIMARY KEY CHECK (id = 1),
			repo_name          TEXT    NOT NULL,
			revision           TEXT    NOT NULL,
			root_catalog_hash  TEXT    NOT NULL,
			scanned_at         TEXT    NOT NULL
		)`)
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to create repo_metadata table: %w", err)
		}
	}

	return &GCDatabase{db: db}, nil
}

// SaveRepoMetadata stores the repository metadata snapshot in the database.
// It replaces any previously stored metadata (there is always at most one row).
func (g *GCDatabase) SaveRepoMetadata(meta RepoMetadata) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := g.db.Exec(`INSERT OR REPLACE INTO repo_metadata
		(id, repo_name, revision, root_catalog_hash, scanned_at)
		VALUES (1, ?, ?, ?, ?)`,
		meta.Name, meta.Revision, meta.RootCatalogHash, now)
	if err != nil {
		return fmt.Errorf("failed to save repo metadata: %w", err)
	}
	return nil
}

// GetRepoMetadataFromDB retrieves the stored repository metadata, if any.
// Returns os.ErrNotExist when no metadata has been recorded yet.
func (g *GCDatabase) GetRepoMetadataFromDB() (RepoMetadata, error) {
	var meta RepoMetadata
	var scannedAt string
	err := g.db.QueryRow(`SELECT repo_name, revision, root_catalog_hash, scanned_at
		FROM repo_metadata WHERE id = 1`).Scan(
		&meta.Name, &meta.Revision, &meta.RootCatalogHash, &scannedAt)
	if err == sql.ErrNoRows {
		return meta, os.ErrNotExist
	}
	if err != nil {
		return meta, fmt.Errorf("failed to read repo metadata: %w", err)
	}
	return meta, nil
}

// Close closes the underlying database connection.
func (g *GCDatabase) Close() error {
	return g.db.Close()
}

// InsertPaths adds a batch of paths with a given category to the database.
// Paths that already exist are silently ignored (UPSERT behaviour).
func (g *GCDatabase) InsertPaths(paths []string, category string) error {
	tx, err := g.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT OR IGNORE INTO gc_paths (path, category, scanned_at) VALUES (?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	now := time.Now().UTC().Format(time.RFC3339)
	for _, p := range paths {
		if _, err := stmt.Exec(p, category, now); err != nil {
			return fmt.Errorf("failed to insert path %s: %w", p, err)
		}
	}

	return tx.Commit()
}

// PendingPaths returns all paths that have not yet been deleted.
func (g *GCDatabase) PendingPaths() ([]string, error) {
	rows, err := g.db.Query(`SELECT path FROM gc_paths WHERE deleted = 0 ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("failed to query pending paths: %w", err)
	}
	defer rows.Close()

	var paths []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		paths = append(paths, p)
	}
	return paths, rows.Err()
}

// MarkDeleted marks a list of paths as deleted in the database.
func (g *GCDatabase) MarkDeleted(paths []string) error {
	tx, err := g.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`UPDATE gc_paths SET deleted = 1 WHERE path = ?`)
	if err != nil {
		return fmt.Errorf("failed to prepare update statement: %w", err)
	}
	defer stmt.Close()

	for _, p := range paths {
		if _, err := stmt.Exec(p); err != nil {
			return fmt.Errorf("failed to mark path %s as deleted: %w", p, err)
		}
	}

	return tx.Commit()
}

// Summary returns counts of pending and deleted paths.
func (g *GCDatabase) Summary() (pending int, deleted int, err error) {
	err = g.db.QueryRow(`SELECT COUNT(*) FROM gc_paths WHERE deleted = 0`).Scan(&pending)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to count pending paths: %w", err)
	}
	err = g.db.QueryRow(`SELECT COUNT(*) FROM gc_paths WHERE deleted = 1`).Scan(&deleted)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to count deleted paths: %w", err)
	}
	return pending, deleted, nil
}
