package db

import (
	"database/sql"
	"flag"
	"io"
	"os"
	"testing"

	_ "embed"

	_ "github.com/mattn/go-sqlite3"
)

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(m.Run())
}

func CreateInMemDBForTesting() *sql.DB {
	// Open in memory database
	db, err := sql.Open("sqlite3", ":memory:?cache=shared")
	if err != nil {
		panic(err)
	}
	return db
}

func CreateInMemDBFromFixture(fixturePath string) *sql.DB {
	db := CreateInMemDBForTesting()

	_, err := os.Stat(fixturePath)
	if os.IsNotExist(err) {
		panic("Fixture file does not exist")
	}

	// Read fixture file
	f, err := os.Open(fixturePath)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(string(data))
	if err != nil {
		panic(err)
	}
	return db
}
