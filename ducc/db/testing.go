package db

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
)

func createInMemDBForTesting() *sql.DB {
	// OPen in memory database
	//db, err := sql.Open("sqlite3", "/root/oystub/ducctest.db")
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		panic(err)
	}
	return db
}
