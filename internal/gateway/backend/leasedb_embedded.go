package backend

import (
	"context"
	"database/sql"
	"fmt"
	"os"
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
	getLeases *sql.Stmt
	getLease  *sql.Stmt
}

// EmbeddedLeaseDB is a LeaseDB backed by BoltDB
type EmbeddedLeaseDB struct {
	store *sql.DB
	locks NamedLocks // Per-repository commit locks
	st    statements
}

// OpenEmbeddedLeaseDB creates a new embedded lease DB
func OpenEmbeddedLeaseDB(workDir string) (*EmbeddedLeaseDB, error) {
	if err := os.MkdirAll(workDir, 0777); err != nil {
		return nil, errors.Wrap(err, "could not create directory for backing store")
	}
	dbFile := workDir + "/lease.db"
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

	gw.Log("leasedb", gw.LogInfo).
		Msgf("database opened (work dir: %v)", workDir)

	return &EmbeddedLeaseDB{
		store: store,
		locks: NamedLocks{},
		st:    statements{getLeases: st1, getLease: st2},
	}, nil
}

// Close the lease database
func (db *EmbeddedLeaseDB) Close() error {
	err1 := db.st.getLeases.Close()
	err2 := db.st.getLease.Close()
	err3 := db.store.Close()
	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}
	if err3 != nil {
		return err3
	}
	return nil
}

// NewLease attemps to acquire a new lease for the given path
func (db *EmbeddedLeaseDB) NewLease(ctx context.Context, keyID, leasePath string, protocolVersion int, token LeaseToken) error {
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
			gw.LogC(ctx, "leasedb", gw.LogDebug).
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

	gw.LogC(ctx, "leasedb", gw.LogDebug).
		Str("operation", "new_lease").
		Str("token", token.TokenStr).
		Float64("task_dt", time.Now().Sub(t0).Seconds()).
		Msgf("key: %v, path: %v", keyID, leasePath)

	return nil
}

// GetLeases returns a list of all active leases
func (db *EmbeddedLeaseDB) GetLeases(ctx context.Context) (map[string]Lease, error) {
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

	gw.LogC(ctx, "leasedb", gw.LogDebug).
		Str("operation", "get_leases").
		Float64("task_dt", time.Now().Sub(t0).Seconds()).
		Msgf("found %v leases", len(leases))

	return leases, nil
}

// GetLease returns the lease for a given token string
func (db *EmbeddedLeaseDB) GetLease(ctx context.Context, tokenStr string) (string, *Lease, error) {
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

	gw.LogC(ctx, "leasedb", gw.LogDebug).
		Str("operation", "get_lease").
		Str("token", tokenStr).
		Float64("task_dt", time.Now().Sub(t0).Seconds()).
		Msgf("success")

	return leasePath, lease, nil
}

// CancelLeases cancels all active leases
func (db *EmbeddedLeaseDB) CancelLeases(ctx context.Context) error {
	t0 := time.Now()

	txn, err := db.store.Begin()
	if err != nil {
		return errors.Wrap(err, "could not begin transaction")
	}
	defer txn.Rollback()

	res, err := txn.Exec("DELETE FROM Leases;")
	if err != nil {
		return errors.Wrap(err, "statement failed")
	}

	numRows, err := res.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "statement result inaccessible")
	}

	if numRows == 0 {
		gw.LogC(ctx, "leasedb", gw.LogDebug).
			Str("operation", "cancel_lease").
			Msgf("cancellation failed")
		return InvalidLeaseError{}
	}

	if err := txn.Commit(); err != nil {
		return errors.Wrap(err, "transaction commit failed")
	}

	gw.LogC(ctx, "leasedb", gw.LogDebug).
		Str("operation", "cancel_leases").
		Float64("task_dt", time.Now().Sub(t0).Seconds()).
		Msgf("all leases cancelled")

	return nil
}

// CancelLease cancels the lease for a token string
func (db *EmbeddedLeaseDB) CancelLease(ctx context.Context, tokenStr string) error {
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
		gw.LogC(ctx, "leasedb", gw.LogDebug).
			Str("operation", "cancel_lease").
			Str("token", tokenStr).
			Msgf("cancellation failed, invalid token")
		return InvalidLeaseError{}
	}

	if err := txn.Commit(); err != nil {
		return errors.Wrap(err, "transaction commit failed")
	}

	gw.LogC(ctx, "leasedb", gw.LogDebug).
		Str("operation", "cancel_lease").
		Str("token", tokenStr).
		Float64("task_dt", time.Now().Sub(t0).Seconds()).
		Msgf("lease cancelled")

	return nil
}

// WithLock runs the given task while holding a commit lock for the repository
func (db *EmbeddedLeaseDB) WithLock(ctx context.Context, repository string, task func() error) error {
	return db.locks.WithLock(repository, task)
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
