package db

import (
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"time"
)

// g_db is a package-global variable that holds the database connection.
var g_db *sql.DB

// Sqlite does not nateively support date/time types, we use ISO8601 format instead.
const ISO8601Format string = "2006-01-02T15:04:05.999999999Z07:00"

// Update this variable when changing the database schema.
const db_schema_version = 3

//go:embed schemas/db_schema_3.sql
var db_schema string

// scanableRow is an interface used to be able to scan both sql.Rows and sql.Row
// from the same function.
type scannableRow interface {
	Scan(dest ...any) error
}

// ValueWithDefault is a value that will take the default value if not set
type ValueWithDefault[T any] struct {
	Value     T    `json:"value"`
	IsDefault bool `json:"isDefault"`
}

// nullBoolToValueWithDefault converts a sql.NullBool to a ValueWithDefault[bool].
// If the sql.NullBool is not valid, the default value is returned.
// Else, the value is returned.
func nullBoolToValueWithDefault(val sql.NullBool, defaultValue bool) ValueWithDefault[bool] {
	if !val.Valid {
		return ValueWithDefault[bool]{
			Value:     defaultValue,
			IsDefault: true,
		}
	}
	return ValueWithDefault[bool]{
		Value:     val.Bool,
		IsDefault: false,
	}
}

func nullSecondsToValueWithDefault(val sql.NullInt64, defaultValue time.Duration) ValueWithDefault[time.Duration] {
	if !val.Valid {
		return ValueWithDefault[time.Duration]{
			Value:     defaultValue,
			IsDefault: true,
		}
	}
	// Convert seconds to duration
	return ValueWithDefault[time.Duration]{
		Value:     time.Duration(val.Int64) * time.Second,
		IsDefault: false,
	}
}

// GetTransaction opens a database transaction, which can be used for calling multiple operations atomically.
// Remember to call `tx.Commit()` or `tx.Rollback()` when done.
func GetTransaction() (*sql.Tx, error) {
	if g_db == nil {
		return nil, errors.New("database not initialized")
	}
	tx, err := g_db.Begin()
	// TODO: Might want to retry, especially if the error is "database is locked"
	if err != nil {
		return nil, err
	}
	return tx, nil
}

// Init initializes the database connection
// If the existing database is not empty, it will be migrated to the latest schema version
func Init(db *sql.DB) error {
	db.SetMaxOpenConns(1)
	var userVersion int
	err := db.QueryRow("PRAGMA user_version").Scan(&userVersion)
	if err != nil {
		fmt.Println("Failed to get user version of the DB:", err)
		return err
	}
	if userVersion == db_schema_version {
		// The DB is already initialized
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
	} else {
		// The DB is empty, we need to create the tables
		_, err = db.Exec(db_schema)
		if err != nil {
			fmt.Println("Failed to create tables in the DB:", err)
			return err
		}
	}

	// Enable foreign key constraints
	_, err = db.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		fmt.Println("Failed to enable foreign key constraints in the DB:", err)
		return err
	}

	g_db = db
	return nil
}

// Close closes the database connection
// If the database is not initialized, this is a no-op
func Close() error {
	if g_db != nil {
		return g_db.Close()
	}
	return nil
}

// ToDBTimeStamp converts a [time.Time] to a string in ISO8601 format
func ToDBTimeStamp(t time.Time) string {
	return t.UTC().Format(ISO8601Format)
}

// FromDBToTimeStamp converts a string in ISO8601 format to a [time.Time]
func FromDBTimeStamp(s string) (time.Time, error) {
	return time.Parse(ISO8601Format, s)
}
