package backend

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	gw "github.com/cvmfs/gateway/internal/gateway"
	_ "github.com/mattn/go-sqlite3" // import SQLite3 driver
	"github.com/pkg/errors"
)

const (
	// latestSchemaVersion represents the most recent lease DB schema version
	// know to the application
	latestSchemaVersion = 1
)

type statements struct {
	getLeases        *sql.Stmt
	getLease         *sql.Stmt
	getLeasesForRepo *sql.Stmt
	getDisabledRepo  *sql.Stmt
}

// SqliteLeaseDB is a LeaseDB backed by BoltDB
type SqliteLeaseDB struct {
	store *sql.DB
	locks NamedLocks // Per-repository commit locks
	st    statements
}

// OpenSqliteLeaseDB creates a new embedded lease DB
func OpenSqliteLeaseDB(workDir string) (*SqliteLeaseDB, error) {
	if err := os.MkdirAll(workDir, 0777); err != nil {
		return nil, errors.Wrap(err, "could not create directory for backing store")
	}
	dbFile := workDir + "/sqlite_lease.db"
	createDB := false
	if _, err := os.Stat(dbFile); os.IsNotExist(err) {
		createDB = true
	}

	store, err := sql.Open("sqlite3", "file:"+dbFile+"?mode=rwc")
	if err != nil {
		return nil, errors.Wrap(err, "could not open backing store (SQLite3)")
	}

	if createDB {
		if err := createSchema(store); err != nil {
			return nil, errors.Wrap(err, "could not initialize backing store (SQLite3)")
		}
	}

	if _, err := checkSchemaVersion(store); err != nil {
		return nil, errors.Wrap(err, "invalid schema version")
	}

	st1, err := store.Prepare("SELECT Token, Repository, Path, KeyID, Secret, Expiration, ProtocolVersion from Leases;")
	if err != nil {
		return nil, errors.Wrap(err, "could not prepare statement for 'get all leases'")
	}
	st2, err := store.Prepare("SELECT Repository, Path, KeyID, Secret, Expiration, ProtocolVersion from Leases WHERE Token = ?;")
	if err != nil {
		return nil, errors.Wrap(err, "could not prepare statement for 'get lease'")
	}
	st3, err := store.Prepare("SELECT Token, Path from Leases WHERE Repository = ?;")
	if err != nil {
		return nil, errors.Wrap(err, "could not prepare statement for 'get lease for repo'")
	}
	st4, err := store.Prepare("SELECT Name from DisabledRepos WHERE Name = ?;")
	if err != nil {
		return nil, errors.Wrap(err, "could not prepare statement for 'get disabled repo'")
	}

	// Enable all repos
	txn, err := store.Begin()
	if err != nil {
		return nil, errors.Wrap(err, "could not begin transaction")
	}
	defer txn.Rollback()

	if _, err := txn.Exec("DELETE FROM DisabledRepos;"); err != nil {
		return nil, errors.Wrap(err, "could not clear DisabledRepos table")
	}

	if err := txn.Commit(); err != nil {
		return nil, errors.Wrap(err, "transaction commit failed")
	}

	gw.Log("leasedb_sqlite", gw.LogInfo).
		Msgf("database opened (work dir: %v)", workDir)

	return &SqliteLeaseDB{
		store: store,
		locks: NamedLocks{},
		st:    statements{getLeases: st1, getLease: st2, getLeasesForRepo: st3, getDisabledRepo: st4},
	}, nil
}

// Close the lease database
func (db *SqliteLeaseDB) Close() error {
	err1 := db.st.getLeases.Close()
	err2 := db.st.getLease.Close()
	err3 := db.st.getLeasesForRepo.Close()
	err4 := db.store.Close()
	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}
	if err3 != nil {
		return err3
	}
	if err4 != nil {
		return err4
	}
	return nil
}

// NewLease attemps to acquire a new lease for the given path
func (db *SqliteLeaseDB) NewLease(ctx context.Context, keyID, leasePath string, protocolVersion int, token LeaseToken) error {
	t0 := time.Now()

	txn, err := db.store.Begin()
	if err != nil {
		return errors.Wrap(err, "could not begin transaction")
	}
	defer txn.Rollback()

	repoName, subPath, err := gw.SplitLeasePath(leasePath)
	if err != nil {
		return errors.Wrap(err, "invalid lease path")
	}

	row := db.st.getDisabledRepo.QueryRow(repoName)
	var n string
	if row.Scan(&n) != sql.ErrNoRows {
		return ErrRepoDisabled
	}

	matches, err := txn.Query(
		"SELECT Token, Path, Expiration FROM Leases WHERE Repository = ?", repoName)
	if err != nil {
		return errors.Wrap(err, "query failed")
	}
	defer matches.Close()

	var existing struct {
		token      string
		expiration time.Time
	}

	for matches.Next() {
		token := ""
		kPath := ""
		var expiration int64
		if err := matches.Scan(&token, &kPath, &expiration); err != nil {
			return errors.Wrap(err, "query scan failed")
		}
		if gw.CheckPathOverlap(kPath, subPath) {
			existing.token = token
			existing.expiration = time.Unix(0, expiration)
			break
		}
	}

	// If a conflicting lease exists, examine its expiration time
	if existing.token != "" {
		timeLeft := existing.expiration.Sub(time.Now())
		if timeLeft > 0 {
			gw.LogC(ctx, "leasedb_sqlite", gw.LogDebug).
				Str("operation", "new_lease").
				Str("token", token.TokenStr).
				Msgf("path_busy, time left: %v s", timeLeft.Seconds())
			return PathBusyError{timeLeft}
		}

		if _, err := txn.Exec(
			"UPDATE Leases SET Token = ?, Repository = ?, Path = ?, KeyID = ?, Secret = ?, Expiration = ?, ProtocolVersion = ? WHERE Token = ?;",
			token.TokenStr, repoName, subPath, keyID, token.Secret, token.Expiration.UnixNano(), protocolVersion, existing.token); err != nil {
			return errors.Wrap(err, "could not update values in backing store")
		}
	} else {
		if _, err := txn.Exec(
			"INSERT INTO Leases (Token, Repository, Path, KeyID, Secret, Expiration, ProtocolVersion) VALUES (?, ?, ?, ?, ?, ?, ?);",
			token.TokenStr, repoName, subPath, keyID, token.Secret, token.Expiration.UnixNano(), protocolVersion); err != nil {
			return errors.Wrap(err, "could not update values in backing store")
		}
	}

	if err := txn.Commit(); err != nil {
		return errors.Wrap(err, "could not commit transaction")
	}

	gw.LogC(ctx, "leasedb_sqlite", gw.LogDebug).
		Str("operation", "new_lease").
		Str("token", token.TokenStr).
		Dur("task_dt", time.Since(t0)).
		Msgf("key: %v, path: %v", keyID, leasePath)

	return nil
}

// GetLeases returns a list of all active leases
func (db *SqliteLeaseDB) GetLeases(ctx context.Context) (map[string]Lease, error) {
	t0 := time.Now()

	matches, err := db.st.getLeases.Query()
	if err != nil {
		return nil, errors.Wrap(err, "query failed")
	}
	defer matches.Close()

	leases := make(map[string]Lease)
	for matches.Next() {
		var tokenStr string
		var repoName string
		var subPath string
		var keyID string
		secret := []byte{}
		var expiration int64
		var protocolVersion int64
		if err := matches.Scan(&tokenStr, &repoName, &subPath, &keyID, &secret, &expiration, &protocolVersion); err != nil {
			return nil, errors.Wrap(err, "query scan failed")
		}
		leasePath := repoName + subPath
		token := LeaseToken{TokenStr: tokenStr, Secret: secret, Expiration: time.Unix(0, expiration)}
		leases[leasePath] = Lease{KeyID: keyID, ProtocolVersion: int(protocolVersion), Token: token}
	}

	gw.LogC(ctx, "leasedb_sqlite", gw.LogDebug).
		Str("operation", "get_leases").
		Dur("task_dt", time.Since(t0)).
		Msgf("found %v leases", len(leases))

	return leases, nil
}

// GetLease returns the lease for a given token string
func (db *SqliteLeaseDB) GetLease(ctx context.Context, tokenStr string) (string, *Lease, error) {
	t0 := time.Now()

	var repoName string
	var subPath string
	var keyID string
	secret := []byte{}
	var expiration int64
	var protocolVersion int64
	err := db.st.getLease.
		QueryRow(tokenStr).
		Scan(&repoName, &subPath, &keyID, &secret, &expiration, &protocolVersion)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil, InvalidLeaseError{}
		}
		return "", nil, errors.Wrap(err, "query failed")
	}

	leasePath := repoName + subPath
	lease := &Lease{
		KeyID:           keyID,
		ProtocolVersion: int(protocolVersion),
		Token:           LeaseToken{TokenStr: tokenStr, Secret: secret, Expiration: time.Unix(0, expiration)},
	}

	gw.LogC(ctx, "leasedb_sqlite", gw.LogDebug).
		Str("operation", "get_lease").
		Str("token", tokenStr).
		Dur("task_dt", time.Since(t0)).
		Msgf("success")

	return leasePath, lease, nil
}

// CancelLeases cancels all active leases
func (db *SqliteLeaseDB) CancelLeases(ctx context.Context, repoPath string) error {
	t0 := time.Now()

	txn, err := db.store.Begin()
	if err != nil {
		return errors.Wrap(err, "could not begin transaction")
	}
	defer txn.Rollback()

	repository, prefix, err := gw.SplitLeasePath(repoPath)
	if err != nil {
		return errors.Wrap(err, "invalid path")
	}

	matches, err := db.st.getLeasesForRepo.Query(repository)
	if err != nil {
		return errors.Wrap(err, "query failed")
	}
	defer matches.Close()

	tokensForDeletion := make([]interface{}, 0)
	for matches.Next() {
		var tokenStr string
		var sp string
		if err := matches.Scan(&tokenStr, &sp); err != nil {
			return errors.Wrap(err, "query scan failed")
		}
		if strings.HasPrefix(sp, prefix) {
			tokensForDeletion = append(tokensForDeletion, tokenStr)
		}
	}

	if len(tokensForDeletion) > 0 {
		placeholders := "("
		for i := 0; i < len(tokensForDeletion)-1; i++ {
			placeholders += "?,"
		}
		placeholders += "?);"

		res, err := txn.Exec("DELETE FROM Leases WHERE Token IN "+placeholders, tokensForDeletion...)
		if err != nil {
			return errors.Wrap(err, "statement failed")
		}

		numRows, err := res.RowsAffected()
		if err != nil {
			return errors.Wrap(err, "statement result inaccessible")
		}

		if numRows == 0 {
			gw.LogC(ctx, "leasedb_sqlite", gw.LogDebug).
				Str("operation", "cancel_lease").
				Msg("cancellation failed")
			return InvalidLeaseError{}
		}
	}

	if err := txn.Commit(); err != nil {
		return errors.Wrap(err, "transaction commit failed")
	}

	gw.LogC(ctx, "leasedb_sqlite", gw.LogDebug).
		Str("operation", "cancel_leases").
		Dur("task_dt", time.Since(t0)).
		Msg("leases cancelled for prefix")

	return nil
}

// CancelLease cancels the lease for a token string
func (db *SqliteLeaseDB) CancelLease(ctx context.Context, tokenStr string) error {
	t0 := time.Now()

	txn, err := db.store.Begin()
	if err != nil {
		return errors.Wrap(err, "could not begin transaction")
	}
	defer txn.Rollback()

	res, err := txn.Exec("DELETE FROM Leases WHERE Token = ?;", tokenStr)
	if err != nil {
		return errors.Wrap(err, "statement failed")
	}

	numRows, err := res.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "statement result inaccessible")
	}

	if numRows == 0 {
		gw.LogC(ctx, "leasedb_sqlite", gw.LogDebug).
			Str("operation", "cancel_lease").
			Str("token", tokenStr).
			Msgf("cancellation failed, invalid token")
		return InvalidLeaseError{}
	}

	if err := txn.Commit(); err != nil {
		return errors.Wrap(err, "transaction commit failed")
	}

	gw.LogC(ctx, "leasedb_sqlite", gw.LogDebug).
		Str("operation", "cancel_lease").
		Str("token", tokenStr).
		Dur("task_dt", time.Since(t0)).
		Msgf("lease cancelled")

	return nil
}

// WithLock runs the given task while holding a commit lock for the repository
func (db *SqliteLeaseDB) WithLock(ctx context.Context, repository string, task func() error) error {
	return db.locks.WithLock(repository, task)
}

// SetRepositoryEnabled sets the enabled/disabled status for a given repository
func (db *SqliteLeaseDB) SetRepositoryEnabled(
	ctx context.Context, repository string, enable bool) error {
	t0 := time.Now()

	txn, err := db.store.Begin()
	if err != nil {
		return errors.Wrap(err, "could not begin transaction")
	}
	defer txn.Rollback()

	if enable {
		if _, err := txn.Exec(
			"DELETE FROM DisabledRepos WHERE Name = ?;", repository); err != nil {
			return errors.Wrap(err, "statement failed")
		}
	} else {
		row := db.st.getDisabledRepo.QueryRow(repository)
		var n string
		if err := row.Scan(&n); err != nil {
			if err == sql.ErrNoRows {
				if _, err := txn.Exec(
					"INSERT INTO DisabledRepos (Name) VALUES (?);", repository); err != nil {
					return errors.Wrap(err, "statement failed")
				}
			} else {
				return errors.Wrap(err, "query failed")
			}
		}
	}

	if err := txn.Commit(); err != nil {
		return errors.Wrap(err, "transaction commit failed")
	}

	gw.LogC(ctx, "leasedb_sqlite", gw.LogDebug).
		Str("operation", "set_repo_enabled").
		Dur("task_dt", time.Since(t0)).
		Str("repository", repository).
		Bool("enabled", enable).
		Msgf("repository status changed")

	return nil
}

// GetRepositoryEnabled returns the enabled status of a repository
func (db *SqliteLeaseDB) GetRepositoryEnabled(ctx context.Context, repository string) bool {
	t0 := time.Now()

	enabled := true

	row := db.st.getDisabledRepo.QueryRow(repository)
	var n string
	if row.Scan(&n) != sql.ErrNoRows {
		enabled = false
	}

	gw.LogC(ctx, "leasedb_sqlite", gw.LogDebug).
		Str("operation", "get_repo_enabled").
		Dur("task_dt", time.Since(t0)).
		Str("repository", repository).
		Bool("enabled", enabled).
		Msgf("repository status queried")

	return enabled
}

func createSchema(db *sql.DB) error {
	statement := fmt.Sprintf(`
CREATE TABLE SchemaVersion (
    VersionNumber integer NOT NULL UNIQUE PRIMARY KEY,
    ValidFrom timestamp NOT NULL,
    ValidTo timestamp
);
INSERT INTO SchemaVersion (VersionNumber, ValidFrom) VALUES (%v, DATETIME('now'));
CREATE TABLE IF NOT EXISTS Leases (
	Token string NOT NULL UNIQUE PRIMARY KEY,
	Repository string NOT NULL,
	Path string NOT NULL,
	KeyID string NOT NULL,
	Secret blob NOT NULL,
	Expiration integer NOT NULL,
	ProtocolVersion integer NOT NULL
);
CREATE TABLE IF NOT EXISTS DisabledRepos (
	Name string NOT NULL UNIQUE PRIMARY KEY
);
`,
		latestSchemaVersion)
	if _, err := db.Exec(statement); err != nil {
		return errors.Wrap(err, "could not create table 'SchemaVersion'")
	}
	return nil
}

func checkSchemaVersion(db *sql.DB) (int, error) {
	var version int
	if err := db.QueryRow(
		"SELECT VersionNumber from SchemaVersion;").Scan(&version); err != nil {
		return 0, errors.Wrap(err, "could not retrieve schema version")
	}

	if version > latestSchemaVersion {
		return 0, fmt.Errorf(
			"unknown schema version: %v, latest known %v",
			version, latestSchemaVersion)
	}

	return version, nil
}
