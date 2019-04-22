package backend

import (
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

// EmbeddedLeaseDB is a LeaseDB backed by BoltDB
type EmbeddedLeaseDB struct {
	store *sql.DB
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

	gw.Log.Info().
		Str("component", "leasedb").
		Msgf("database opened (work dir: %v)", workDir)

	return &EmbeddedLeaseDB{store}, nil
}

// Close the lease database
func (db *EmbeddedLeaseDB) Close() error {
	return db.store.Close()
}

// NewLease attemps to acquire a new lease for the given path
func (db *EmbeddedLeaseDB) NewLease(keyID, leasePath string, token LeaseToken) error {
	txn, err := db.store.Begin()
	if err != nil {
		return errors.Wrap(err, "could not begin transaction")
	}
	defer txn.Rollback()

	repoName, subPath, err := SplitLeasePath(leasePath)
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
		if CheckPathOverlap(kPath, subPath) {
			existing.token = token
			existing.expiration = time.Unix(0, expiration)
			break
		}
	}

	// If a conflicting lease exists, examine its expiration time
	if existing.token != "" {
		timeLeft := existing.expiration.Sub(time.Now())
		if timeLeft > 0 {
			gw.Log.Debug().
				Str("component", "leasedb").
				Msgf("new lease request failed: path_busy, time left: %v s", timeLeft.Seconds())
			return PathBusyError{timeLeft}
		}

		if _, err := txn.Exec(
			"UPDATE Leases SET Token = ?, Repository = ?, Path = ?, KeyID = ?, Secret = ?, Expiration = ? WHERE Token = ?;",
			token.TokenStr, repoName, subPath, keyID, token.Secret, token.Expiration.UnixNano(), existing.token); err != nil {
			return errors.Wrap(err, "could not update values in backing store")
		}
	} else {
		if _, err := txn.Exec(
			"INSERT INTO Leases (Token, Repository, Path, KeyID, Secret, Expiration) VALUES (?, ?, ?, ?, ?, ?);",
			token.TokenStr, repoName, subPath, keyID, token.Secret, token.Expiration.UnixNano()); err != nil {
			return errors.Wrap(err, "could not update values in backing store")
		}
	}

	gw.Log.Debug().
		Str("component", "leasedb").
		Msgf("lease granted with key: %v, path: %v", keyID, leasePath)

	if err := txn.Commit(); err != nil {
		return errors.Wrap(err, "could not commit transaction")
	}

	return nil
}

// GetLeases returns a list of all active leases
func (db *EmbeddedLeaseDB) GetLeases() (map[string]Lease, error) {
	matches, err := db.store.Query("SELECT Token, Repository, Path, KeyID, Secret, Expiration from Leases;")
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
		if err := matches.Scan(&tokenStr, &repoName, &subPath, &keyID, &secret, &expiration); err != nil {
			return nil, errors.Wrap(err, "query scan failed")
		}
		leasePath := repoName + subPath
		token := LeaseToken{TokenStr: tokenStr, Secret: secret, Expiration: time.Unix(0, expiration)}
		leases[leasePath] = Lease{KeyID: keyID, Token: token}
	}

	return leases, nil
}

// GetLease returns the lease for a given token string
func (db *EmbeddedLeaseDB) GetLease(tokenStr string) (string, *Lease, error) {
	var repoName string
	var subPath string
	var keyID string
	secret := []byte{}
	var expiration int64

	err := db.store.
		QueryRow("SELECT Repository, Path, KeyID, Secret, Expiration from Leases WHERE Token = ?;", tokenStr).
		Scan(&repoName, &subPath, &keyID, &secret, &expiration)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil, InvalidLeaseError{}
		}
		return "", nil, errors.Wrap(err, "query failed")
	}

	leasePath := repoName + subPath
	lease := &Lease{
		KeyID: keyID,
		Token: LeaseToken{TokenStr: tokenStr, Secret: secret, Expiration: time.Unix(0, expiration)},
	}
	return leasePath, lease, nil
}

// CancelLeases cancels all active leases
func (db *EmbeddedLeaseDB) CancelLeases() error {
	txn, err := db.store.Begin()
	if err != nil {
		return errors.Wrap(err, "could not begin transaction")
	}
	defer txn.Rollback()

	if _, err := txn.Exec("DELETE FROM Leases;"); err != nil {
		return errors.Wrap(err, "statement failed")
	}

	if err := txn.Commit(); err != nil {
		return errors.Wrap(err, "transaction commit failed")
	}

	gw.Log.Debug().
		Str("component", "leasedb").
		Msgf("all leases cancelled")

	return nil
}

// CancelLease cancels the lease for a token string
func (db *EmbeddedLeaseDB) CancelLease(tokenStr string) error {
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
		gw.Log.Debug().
			Str("component", "leasedb").
			Msgf("cancellation failed, invalid token: %v", tokenStr)
		return InvalidLeaseError{}
	}

	if err := txn.Commit(); err != nil {
		return errors.Wrap(err, "transaction commit failed")
	}

	gw.Log.Debug().
		Str("component", "leasedb").
		Msgf("lease cancelled for token: %v", tokenStr)

	return nil
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
	Expiration integer NOT NULL
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
