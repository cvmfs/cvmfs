package db

import (
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
)

const db_schema_version = 2

//go:embed schemas/db_schema_2.sql
var db_schema string

type scannableRow interface {
	Scan(dest ...any) error
}

// Open opens a database transaction, which can be used for calling multiple operations atomically.
// Remember to call `tx.Commit()` or `tx.Rollback()` when done.
func GetTransaction() (*sql.Tx, error) {
	if g_db == nil {
		return nil, errors.New("database not initialized")
	}
	tx, err := g_db.Begin()
	if err != nil {
		panic(err)
	}
	return tx, nil
}

var g_db *sql.DB

func Init(db *sql.DB) error {
	var userVersion int
	err := db.QueryRow("PRAGMA user_version").Scan(&userVersion)
	if err != nil {
		fmt.Println("Failed to get user version of the DB:", err)
		return err
	}
	if userVersion == db_schema_version {
		// The DB is already initialized
		return nil
	} else if userVersion != 0 {
		// The DB is not empty, but the schema version is not the one we expect
		if migrationSupported(userVersion, db_schema_version) {
			err := performMigration(db, userVersion, db_schema_version)
			if err != nil {
				return err
			}
		} else {
			return fmt.Errorf("DB schema version is %d but we expect %d. Migration not supported", userVersion, db_schema_version)
		}
	}

	// The DB is empty, we need to create the tables
	_, err = db.Exec(db_schema)
	if err != nil {
		fmt.Println("Failed to create tables in the DB:", err)
		return err
	}

	g_db = db
	return nil
}
