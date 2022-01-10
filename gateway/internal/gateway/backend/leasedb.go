package backend

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	gw "github.com/cvmfs/gateway/internal/gateway"

	_ "github.com/mattn/go-sqlite3"
)

const (
	// latestSchemaVersion represents the most recent lease DB schema version
	// know to the application
	latestSchemaVersion = 1
)

// PathBusyError is returned as error value for new lease requests on
// paths which are already leased
type PathBusyError struct {
	remaining time.Duration
}

func (e PathBusyError) Error() string {
	return fmt.Sprintf("path busy, remaining = %vs", e.remaining.Seconds())
}

// Remaining number of seconds on the existing lease
func (e *PathBusyError) Remaining() time.Duration {
	return e.remaining
}

// InvalidLeaseError is returned by the GetLeaseXXXX methods in case a
// lease does not exist for the specified path
type InvalidLeaseError struct {
}

func (e InvalidLeaseError) Error() string {
	return "invalid lease"
}

// ErrRepoDisabled signals that a new lease cannot be acquired due to the repository
// being disabled
var ErrRepoDisabled = fmt.Errorf("repo_disabled")

// Lease describes an exclusive lease to a subpath inside the repository:
// keyID and token ()
type Lease struct {
	KeyID           string
	ProtocolVersion int
	Token           LeaseToken
}

// Serialize the lease to a byte buffer
func (e *Lease) Serialize() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	if err := enc.Encode(*e); err != nil {
		return nil, fmt.Errorf("serialization error: %w", err)
	}

	return buf.Bytes(), nil
}

// DeserializeLease from a byte buffer
func DeserializeLease(buf []byte) (*Lease, error) {
	lease := Lease{}
	dec := gob.NewDecoder(bytes.NewReader(buf))
	if err := dec.Decode(&lease); err != nil {
		return nil, fmt.Errorf("deserialization error: %w", err)
	}

	return &lease, nil
}

type statements struct {
	getLeases        *sql.Stmt
	getLease         *sql.Stmt
	getLeasesForRepo *sql.Stmt
	getDisabledRepo  *sql.Stmt
}

// SqliteLeaseDB is a LeaseDB backed by BoltDB
type LeaseDB struct {
	store *sql.DB
	locks NamedLocks // Per-repository commit locks
	st    statements
}

// OpenLeaseDB opens or creats a new LeaseDB object of the specified type
// (either "embedded" or "etcd").
func OpenLeaseDB(dbType string, config *gw.Config) (*LeaseDB, error) {
	if err := os.MkdirAll(config.WorkDir, 0777); err != nil {
		return nil, fmt.Errorf("could not create directory for backing store: %w", err)
	}
	dbFile := config.WorkDir + "/sqlite_lease.db"
	createDB := false
	if _, err := os.Stat(dbFile); os.IsNotExist(err) {
		createDB = true
	}

	store, err := sql.Open("sqlite3", "file:"+dbFile+"?mode=rwc")
	if err != nil {
		return nil, fmt.Errorf("could not open backing store (SQLite3): %w", err)
	}

	if createDB {
		if err := createSchema(store); err != nil {
			return nil, fmt.Errorf("could not initialize backing store (SQLite3): %w", err)
		}
	}

	if _, err := checkSchemaVersion(store); err != nil {
		return nil, fmt.Errorf("invalid schema version: %w", err)
	}

	st1, err := store.Prepare("SELECT Token, Repository, Path, KeyID, Secret, Expiration, ProtocolVersion from Leases;")
	if err != nil {
		return nil, fmt.Errorf("could not prepare statement for 'get all leases': %w", err)
	}
	st2, err := store.Prepare("SELECT Repository, Path, KeyID, Secret, Expiration, ProtocolVersion from Leases WHERE Token = ?;")
	if err != nil {
		return nil, fmt.Errorf("could not prepare statement for 'get lease': %w", err)
	}
	st3, err := store.Prepare("SELECT Token, Path from Leases WHERE Repository = ?;")
	if err != nil {
		return nil, fmt.Errorf("could not prepare statement for 'get lease for repo': %w", err)
	}
	st4, err := store.Prepare("SELECT Name from DisabledRepos WHERE Name = ?;")
	if err != nil {
		return nil, fmt.Errorf("could not prepare statement for 'get disabled repo': %w", err)
	}

	// Enable all repos
	txn, err := store.Begin()
	if err != nil {
		return nil, fmt.Errorf("could not begin transaction: %w", err)
	}
	defer txn.Rollback()

	if _, err := txn.Exec("DELETE FROM DisabledRepos;"); err != nil {
		return nil, fmt.Errorf("could not clear DisabledRepos table: %w", err)
	}

	if err := txn.Commit(); err != nil {
		return nil, fmt.Errorf("transaction commit failed: %w", err)
	}

	gw.Log("leasedb_sqlite", gw.LogInfo).
		Msgf("database opened (work dir: %v)", config.WorkDir)

	return &LeaseDB{
		store: store,
		locks: NamedLocks{},
		st:    statements{getLeases: st1, getLease: st2, getLeasesForRepo: st3, getDisabledRepo: st4},
	}, nil
}

// Close the lease database
func (db *LeaseDB) Close() error {
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
func (db *LeaseDB) NewLease(ctx context.Context, keyID, leasePath string, protocolVersion int, token LeaseToken) error {
	t0 := time.Now()

	txn, err := db.store.Begin()
	if err != nil {
		return fmt.Errorf("could not begin transaction: %w", err)
	}
	defer txn.Rollback()

	repoName, subPath, err := gw.SplitLeasePath(leasePath)
	if err != nil {
		return fmt.Errorf("invalid lease path: %w", err)
	}

	row := db.st.getDisabledRepo.QueryRow(repoName)
	var n string
	if row.Scan(&n) != sql.ErrNoRows {
		return ErrRepoDisabled
	}

	matches, err := txn.Query(
		"SELECT Token, Path, Expiration FROM Leases WHERE Repository = ?", repoName)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
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
			return fmt.Errorf("query scan failed: %w", err)
		}
		if gw.CheckPathOverlap(kPath, subPath) {
			existing.token = token
			existing.expiration = time.Unix(0, expiration)
			break
		}
	}

	// If a conflicting lease exists, examine its expiration time
	if existing.token != "" {
		timeLeft := time.Until(existing.expiration)
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
			return fmt.Errorf("could not update values in backing store: %w", err)
		}
	} else {
		if _, err := txn.Exec(
			"INSERT INTO Leases (Token, Repository, Path, KeyID, Secret, Expiration, ProtocolVersion) VALUES (?, ?, ?, ?, ?, ?, ?);",
			token.TokenStr, repoName, subPath, keyID, token.Secret, token.Expiration.UnixNano(), protocolVersion); err != nil {
			return fmt.Errorf("could not update values in backing store: %w", err)
		}
	}

	if err := txn.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	gw.LogC(ctx, "leasedb_sqlite", gw.LogDebug).
		Str("operation", "new_lease").
		Str("token", token.TokenStr).
		Dur("task_dt", time.Since(t0)).
		Msgf("key: %v, path: %v", keyID, leasePath)

	return nil
}

// GetLeases returns a list of all active leases
func (db *LeaseDB) GetLeases(ctx context.Context) (map[string]Lease, error) {
	t0 := time.Now()

	matches, err := db.st.getLeases.Query()
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
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
			return nil, fmt.Errorf("query scan failed: %w", err)
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
func (db *LeaseDB) GetLease(ctx context.Context, tokenStr string) (string, *Lease, error) {
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
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil, InvalidLeaseError{}
		}
		return "", nil, fmt.Errorf("query failed: %w", err)
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
func (db *LeaseDB) CancelLeases(ctx context.Context, repoPath string) error {
	t0 := time.Now()

	txn, err := db.store.Begin()
	if err != nil {
		return fmt.Errorf("could not begin transaction: %w", err)
	}
	defer txn.Rollback()

	repository, prefix, err := gw.SplitLeasePath(repoPath)
	if err != nil {
		return fmt.Errorf("invalid path: %w", err)
	}

	matches, err := db.st.getLeasesForRepo.Query(repository)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer matches.Close()

	tokensForDeletion := make([]interface{}, 0)
	for matches.Next() {
		var tokenStr string
		var sp string
		if err := matches.Scan(&tokenStr, &sp); err != nil {
			return fmt.Errorf("query scan failed: %w", err)
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
			return fmt.Errorf("statement failed: %w", err)
		}

		numRows, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("statement result inaccessible: %w", err)
		}

		if numRows == 0 {
			gw.LogC(ctx, "leasedb_sqlite", gw.LogDebug).
				Str("operation", "cancel_lease").
				Msg("cancellation failed")
			return InvalidLeaseError{}
		}
	}

	if err := txn.Commit(); err != nil {
		return fmt.Errorf("transaction commit failed: %w", err)
	}

	gw.LogC(ctx, "leasedb_sqlite", gw.LogDebug).
		Str("operation", "cancel_leases").
		Dur("task_dt", time.Since(t0)).
		Msg("leases cancelled for prefix")

	return nil
}

// CancelLease cancels the lease for a token string
func (db *LeaseDB) CancelLease(ctx context.Context, tokenStr string) error {
	t0 := time.Now()

	txn, err := db.store.Begin()
	if err != nil {
		return fmt.Errorf("could not begin transaction: %w", err)
	}
	defer txn.Rollback()

	res, err := txn.Exec("DELETE FROM Leases WHERE Token = ?;", tokenStr)
	if err != nil {
		return fmt.Errorf("statement failed: %w", err)
	}

	numRows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("statement result inaccessible: %w", err)
	}

	if numRows == 0 {
		gw.LogC(ctx, "leasedb_sqlite", gw.LogDebug).
			Str("operation", "cancel_lease").
			Str("token", tokenStr).
			Msgf("cancellation failed, invalid token")
		return InvalidLeaseError{}
	}

	if err := txn.Commit(); err != nil {
		return fmt.Errorf("transaction commit failed: %w", err)
	}

	gw.LogC(ctx, "leasedb_sqlite", gw.LogDebug).
		Str("operation", "cancel_lease").
		Str("token", tokenStr).
		Dur("task_dt", time.Since(t0)).
		Msgf("lease cancelled")

	return nil
}

// WithLock runs the given task while holding a commit lock for the repository
func (db *LeaseDB) WithLock(ctx context.Context, repository string, task func() error) error {
	return db.locks.WithLock(repository, task)
}

// SetRepositoryEnabled sets the enabled/disabled status for a given repository
func (db *LeaseDB) SetRepositoryEnabled(
	ctx context.Context, repository string, enable bool) error {
	t0 := time.Now()

	txn, err := db.store.Begin()
	if err != nil {
		return fmt.Errorf("could not begin transaction: %w", err)
	}
	defer txn.Rollback()

	if enable {
		if _, err := txn.Exec(
			"DELETE FROM DisabledRepos WHERE Name = ?;", repository); err != nil {
			return fmt.Errorf("statement failed: %w", err)
		}
	} else {
		row := db.st.getDisabledRepo.QueryRow(repository)
		var n string
		if err := row.Scan(&n); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				if _, err := txn.Exec(
					"INSERT INTO DisabledRepos (Name) VALUES (?);", repository); err != nil {
					return fmt.Errorf("statement failed: %w", err)
				}
			} else {
				return fmt.Errorf("query failed: %w", err)
			}
		}
	}

	if err := txn.Commit(); err != nil {
		return fmt.Errorf("transaction commit failed: %w", err)
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
func (db *LeaseDB) GetRepositoryEnabled(ctx context.Context, repository string) bool {
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
		return fmt.Errorf("could not create table 'SchemaVersion': %w", err)
	}
	return nil
}

func checkSchemaVersion(db *sql.DB) (int, error) {
	var version int
	if err := db.QueryRow(
		"SELECT VersionNumber from SchemaVersion;").Scan(&version); err != nil {
		return 0, fmt.Errorf("could not retrieve schema version: %w", err)
	}

	if version > latestSchemaVersion {
		return 0, fmt.Errorf(
			"unknown schema version: %v, latest known %v",
			version, latestSchemaVersion)
	}

	return version, nil
}
