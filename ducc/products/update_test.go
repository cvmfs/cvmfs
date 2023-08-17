package products

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/cvmfs/ducc/db"
	"github.com/cvmfs/ducc/registry"
	"github.com/cvmfs/ducc/rest"
)

func TestUpdate(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)
	testdb, err := sql.Open("sqlite3", tempDir+"/test.db")
	fmt.Printf("Running database in %s\n", tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer testdb.Close()
	db.Init(testdb)

	go rest.RunRestApi()

	// Create a test image
	// https://registry.hub.docker.com/atlas/rucio-clients:latest
	testImage := db.Image{
		RegistryScheme: "https",
		RegistryHost:   "ghcr.io",
		Repository:     "oystub/test",
		Tag:            "latest",
	}

	/*testImage := db.Image{
		RegistryScheme: "https",
		RegistryHost:   "registry.hub.docker.com",
		Repository:     "library/alpine",
		Tag:            "latest",
	}*/

	testImage, err = db.CreateImage(nil, testImage)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := registry.FetchManifestAndSaveDigest(testImage)
	if err != nil {
		t.Fatal(err)
	}

	outputOptions := db.WishOutputOptions{
		CreateFlat:      db.ValueWithDefault[bool]{Value: true, IsDefault: true},
		CreateLayers:    db.ValueWithDefault[bool]{Value: true, IsDefault: true},
		CreatePodman:    db.ValueWithDefault[bool]{Value: true, IsDefault: true},
		CreateThinImage: db.ValueWithDefault[bool]{Value: false, IsDefault: false},
	}

	fullUpdateTask, err := FullUpdate(testImage, manifest, outputOptions, "local.test.repo")
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("http://localhost:8080/html/jobs/%s\n", fullUpdateTask.GetValue().ID)

	fullUpdateTask.Start(nil)
	result := fullUpdateTask.WaitUntilDone()
	fmt.Printf("Result: %s\n", result)

	select {}
}
