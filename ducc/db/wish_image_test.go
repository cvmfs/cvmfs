package db

import (
	"testing"

	"github.com/cvmfs/ducc/test"
)

func TestUpdateImagesAndGetImages(t *testing.T) {
	db := CreateInMemDBForTesting()
	Init(db)
	defer db.Close()

	dbWishIdentifiers := []WishIdentifier{
		{
			Wishlist:              "source",
			CvmfsRepository:       "cvmfs",
			InputTag:              "tag*",
			InputTagWildcard:      true,
			InputRepository:       "repository",
			InputRegistryScheme:   "https",
			InputRegistryHostname: "registry",
		},
	}
	dbWishes, err := CreateWishesByIdentifiers(nil, dbWishIdentifiers)
	if err != nil {
		t.Fatal(err)
	}

	dbImageIdentifiers := []Image{
		{
			RegistryScheme: "https",
			RegistryHost:   "registry",
			Repository:     "repository",
			Tag:            "tag1",
		},
		{
			RegistryScheme: "https",
			RegistryHost:   "registry",
			Repository:     "repository",
			Tag:            "tag2",
		},
	}
	dbImages, err := CreateImages(nil, dbImageIdentifiers)
	if err != nil {
		t.Fatal(err)
	}

	toUpdateImages := []Image{
		dbImageIdentifiers[0],
		dbImageIdentifiers[1],
		{
			RegistryScheme: "https",
			RegistryHost:   "registry",
			Repository:     "repository",
			Tag:            "tag3",
		},
	}

	created, updated, unlinked, err := UpdateImagesForWish(nil, dbWishes[0].ID, toUpdateImages)
	if err != nil {
		t.Fatal(err)
	}

	if len(created) != 1 {
		t.Fatalf("Expected %d created images, got %d", 1, len(created))
	}
	if !test.EqualsExceptForFields(toUpdateImages[2], created[0], []string{"ID"}) {
		t.Fatalf("Expected %v, got %v", toUpdateImages[2], created[0])
	}

	if len(updated) != 2 {
		t.Fatalf("Expected %d updated images, got %d", 0, len(updated))
	}
	if !test.EqualsExceptForFields(dbImages[0], updated[0], []string{}) {
		t.Fatalf("Expected %v, got %v", dbImages[0], updated[0])
	}
	if !test.EqualsExceptForFields(dbImages[1], updated[1], []string{}) {
		t.Fatalf("Expected %v, got %v", dbImages[1], updated[1])
	}

	if len(unlinked) != 0 {
		t.Fatalf("Expected %d unlinked images, got %d", 0, len(unlinked))
	}

	// Check that the images are linked to the wish
	images, err := GetImagesByWishID(nil, dbWishes[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(images) != 3 {
		t.Fatalf("Expected %d images, got %d", 3, len(images))
	}

	// Ensure that unlinking works
	created, updated, unlinked, err = UpdateImagesForWish(nil, dbWishes[0].ID, toUpdateImages[2:])
	if err != nil {
		t.Fatal(err)
	}
	if len(created) != 0 || len(updated) != 0 || len(unlinked) != 2 {
		t.Fatalf("Expected 0 created, 0 updated, 2 unlinked images, got %d created, %d updated, %d unlinked", len(created), len(updated), len(unlinked))
	}
	images, err = GetImagesByWishID(nil, dbWishes[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(images) != 1 {
		t.Fatalf("Expected %d images, got %d", 1, len(images))
	}
	if !test.EqualsExceptForFields(toUpdateImages[2], images[0], []string{"ID"}) {
		t.Fatalf("Expected %v, got %v", toUpdateImages[2], images[0])
	}
}
