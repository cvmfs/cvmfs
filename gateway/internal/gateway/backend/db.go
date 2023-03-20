package backend

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	gw "github.com/cvmfs/gateway/internal/gateway"
	_ "github.com/mattn/go-sqlite3"
)

const (
	// latestSchemaVersion represents the most recent lease DB schema version
	// known to the application
	latestSchemaVersion = 3
)

// DB stores active leases
type DB struct {
	SQL   *sql.DB
	Locks NamedLocks // Per-repository commit locks
}

// OpenDB opens or creates the gateway SQL DB
func OpenDB(config gw.Config) (*DB, error) {
	if err := os.MkdirAll(config.WorkDir, 0777); err != nil {
		return nil, fmt.Errorf("could not create working directory: %w", err)
	}
	dbFile := config.WorkDir + "/gw.db"
	createDB := false
	if _, err := os.Stat(dbFile); os.IsNotExist(err) {
		createDB = true
	}

	sqlDB, err := sql.Open("sqlite3", "file:"+dbFile+"?mode=rwc")
	if err != nil {
		return nil, fmt.Errorf("could not open DB: %w", err)
	}

	if createDB {
		if err := createSchema(sqlDB); err != nil {
			return nil, fmt.Errorf("could not initialise DB: %w", err)
		}
	}

	if _, err := checkSchemaVersion(sqlDB); err != nil {
		return nil, fmt.Errorf("invalid schema version: %w", err)
	}

	gw.Log("leasedb", gw.LogInfo).
		Msgf("database opened (work dir: %v)", config.WorkDir)

	return &DB{
		SQL:   sqlDB,
		Locks: NamedLocks{},
	}, nil
}

// Close the lease database
func (db *DB) Close() error {
	errs := make(map[string]error)
	err := db.SQL.Close()
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	if err != nil {
		return err
	}
	return nil
}

// WithLock runs the given task while holding a commit lock for the repository
func (db *DB) WithLock(ctx context.Context, repository string, task func() error) error {
	return db.Locks.WithLock(repository, task)
}

func createSchema(db *sql.DB) error {
	statement := fmt.Sprintf(`
create table SchemaVersion (
    VersionNumber integer not null unique primary key,
    ValidFrom timestamp not null,
    ValidTo timestamp
);
insert into SchemaVersion (VersionNumber, ValidFrom) values (%v, datetime('now'));
create table if not exists Lease (
	Token string not null unique primary key,
	Repository string not null,
	Path string not null,
	KeyID string not null,
	Expiration integer not null,
	ProtocolVersion integer not null,
	Hostname string
);
create index lease_repository_path_idx ON Lease(Repository,Path);
create table if not exists Repository (
	Name string not null unique primary key,
	Manifest string,
	Enabled bool not null
);
`,
		latestSchemaVersion)
	if _, err := db.Exec(statement); err != nil {
		return fmt.Errorf("could not create table 'SchemaVersion': %w", err)
	}
	return nil
}

func checkSchemaVersion(db *sql.DB) (int, error) {
	var version int
	if err := db.QueryRow(
		"select VersionNumber from SchemaVersion;").Scan(&version); err != nil {
		return 0, fmt.Errorf("could not retrieve schema version: %w", err)
	}

	if version > latestSchemaVersion {
		return 0, fmt.Errorf(
			"unknown schema version: %v, latest known %v",
			version, latestSchemaVersion)
	}

	if version == 2 {
		statement := `
alter table lease add column hostname string;
update SchemaVersion set VersionNumber=3, ValidFrom=datetime('now');
`
		if _, err := db.Exec(statement); err != nil {
			return 2, fmt.Errorf("could not migrate table schema (2->3): %w", err)
		}

		version = 3
	}

	return version, nil
}
