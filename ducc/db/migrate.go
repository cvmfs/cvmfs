package db

import "database/sql"

func migrationSupported(fromVersion int, toVersion int) bool {
	return false
}

func performMigration(db *sql.DB, fromVersion int, toVersion int) error {
	return nil
}
