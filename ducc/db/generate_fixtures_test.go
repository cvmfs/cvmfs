package db

import (
	"database/sql"
	"os"
	"os/exec"
	"testing"

	"github.com/cvmfs/ducc/test"
	_ "github.com/mattn/go-sqlite3"
)

func TestGenerateFixture1(t *testing.T) {
	if !*test.UpdateTests {
		t.Skip("Using existing fixtures. Run with -update to update them")
	}

	tempDir, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)
	testdb, err := sql.Open("sqlite3", tempDir+"/fixture.db")
	if err != nil {
		t.Fatal(err)
	}
	defer testdb.Close()

	Init(testdb)

	tx, err := GetTransaction()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	wishesIdentifiers := []WishIdentifier{
		{
			Source:                "source",
			CvmfsRepository:       "cvmfs",
			InputTag:              "tag",
			InputTagWildcard:      false,
			InputRepository:       "repository",
			InputRegistryScheme:   "https",
			InputRegistryHostname: "registry",
		},
		{
			Source:                "source2",
			CvmfsRepository:       "cvmfs",
			InputTag:              "tag",
			InputTagWildcard:      false,
			InputRepository:       "repository",
			InputRegistryScheme:   "https",
			InputRegistryHostname: "registry",
		},
		{
			Source:                "source",
			CvmfsRepository:       "cvmfs",
			InputTag:              "*",
			InputTagWildcard:      true,
			InputRepository:       "repository2",
			InputRegistryScheme:   "https",
			InputRegistryHostname: "registry",
		},
	}

	imagesToCreate := []Image{
		{
			RegistryScheme: "https",
			RegistryHost:   "registry",
			Repository:     "repository",
			Tag:            "tag",
		},
		{
			RegistryScheme: "https",
			RegistryHost:   "registry",
			Repository:     "repository2",
			Tag:            "latest",
		},
		{
			RegistryScheme: "https",
			RegistryHost:   "registry",
			Repository:     "repository2",
			Tag:            "debug",
		},
	}

	wishes, err := CreateWishesByIdentifiers(tx, wishesIdentifiers)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := UpdateImagesForWish(tx, wishes[0].ID, imagesToCreate[0:1]); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := UpdateImagesForWish(tx, wishes[1].ID, imagesToCreate[0:1]); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := UpdateImagesForWish(tx, wishes[2].ID, imagesToCreate[1:]); err != nil {
		t.Fatal(err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	testdb.Close()
	updateDbFixture(t, tempDir+"/fixture.db", "fixture1")
}

func updateDbFixture(t *testing.T, dbPath string, fixtureName string) {
	out, err := exec.Command("sqlite3", dbPath, ".dump").Output()
	if err != nil {
		t.Fatal(err)
	}
	err = os.Mkdir("testdata", 0755)
	if err != nil && !os.IsExist(err) {
		t.Fatal(err)
	}
	f, err := os.OpenFile("testdata/"+fixtureName+".sql", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write(out)
	f.Close()
}
