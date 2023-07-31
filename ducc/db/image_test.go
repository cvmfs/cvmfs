package db

import (
	"database/sql"
	"fmt"
	"reflect"
	"testing"

	"github.com/cvmfs/ducc/test"
	"github.com/google/uuid"
	"github.com/opencontainers/go-digest"
)

func TestCreateImage(t *testing.T) {
	db := createInMemDBForTesting()
	Init(db)
	defer db.Close()

	t.Run("CreateImage", func(t *testing.T) {
		inputImage := Image{
			RegistryScheme: "https",
			RegistryHost:   "registry",
			Repository:     "repository",
			Tag:            "latest",
		}
		image, err := CreateImage(nil, inputImage)
		if err != nil {
			t.Fatal(err)
		}
		if !test.EqualsExceptForFields(inputImage, image, []string{"ID"}) {
			t.Fatalf("Expected %v, got %v", inputImage, image)
		}
		if image.ID == (ImageID{}) {
			t.Fatal("Expected ID to be set")
		}
	})
}

func TestCreateImages(t *testing.T) {
	db := createInMemDBForTesting()
	Init(db)
	defer db.Close()

	t.Run("CreateImages", func(t *testing.T) {
		inputImages := []Image{
			{
				RegistryScheme: "https",
				RegistryHost:   "registry",
				Repository:     "repository",
				Tag:            "latest",
			},
			{
				RegistryScheme: "https",
				RegistryHost:   "registry",
				Repository:     "repository",
				Tag:            "tag2",
			},
		}
		images, err := CreateImages(nil, inputImages)
		if err != nil {
			t.Fatal(err)
		}
		if len(images) != len(inputImages) {
			t.Fatalf("Expected %d images, got %d", len(inputImages), len(images))
		}
		for i := range images {
			if !test.EqualsExceptForFields(inputImages[i], images[i], []string{"ID"}) {
				t.Fatalf("Expected %v, got %v", inputImages[i], images[i])
			}
			if images[i].ID == (ImageID{}) {
				t.Fatal("Expected ID to be set")
			}
		}
		if images[0].ID == images[1].ID {
			t.Fatal("Expected IDs to be different")
		}
	})
}

func TestGetImageByValue(t *testing.T) {
	db := createInMemDBForTesting()
	Init(db)
	defer db.Close()

	inputImages := []Image{
		{
			RegistryScheme: "https",
			RegistryHost:   "registry",
			Repository:     "repository",
			Tag:            "latest",
		},
		{
			RegistryScheme: "http",
			RegistryHost:   "registry2",
			Repository:     "repository2",
			Digest:         digest.FromString(""),
		},
	}
	inputImages, err := CreateImages(nil, inputImages)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("Created images: %v\n", inputImages)

	t.Run("GetExistingImage", func(t *testing.T) {
		image := inputImages[0]
		out, err := GetImageByValue(nil, image)
		if err != nil {
			t.Fatal(err)
		}
		if !test.EqualsExceptForFields(image, out, []string{"ID"}) {
			t.Fatalf("Expected %v, got %v", image, out)
		}
	})

	t.Run("GetNonexistingImage", func(t *testing.T) {
		originalImage := inputImages[0]
		alternateImage := inputImages[1]
		fieldsToChange := []string{"RegistryScheme", "RegistryHost", "Repository", "Tag", "Digest"}
		for _, field := range fieldsToChange {
			image := originalImage
			// Use reflection to change the field from the original image to the alternate image
			reflect.ValueOf(&image).Elem().FieldByName(field).Set(reflect.ValueOf(alternateImage).FieldByName(field))
			_, err := GetImageByValue(nil, image)
			if err != sql.ErrNoRows {
				t.Fatalf("Expected sql.ErrNoRows, got %v", err)
			}
		}
	})

	t.Run("GetImageWithIDSet", func(t *testing.T) {
		inputImage := inputImages[0]
		inputImage.ID = ImageID(uuid.New())
		image, err := GetImageByValue(nil, inputImage)
		if err != nil {
			t.Fatal(err)
		}
		if !test.EqualsExceptForFields(inputImage, image, []string{"ID"}) {
			t.Fatalf("Expected %v, got %v", inputImage, image)
		}
		if image.ID == (ImageID{}) {
			t.Fatal("Expected ID to be set")
		}
		if image.ID == inputImage.ID {
			t.Fatal("Expected unique ID")
		}
	})

	t.Run("GetMultipleImages", func(t *testing.T) {
		images, err := GetImagesByValues(nil, inputImages)
		if err != nil {
			t.Fatal(err)
		}
		if len(images) != len(inputImages) {
			t.Fatalf("Expected %d images, got %d", len(inputImages), len(images))
		}
		for i := range images {
			if !test.EqualsExceptForFields(inputImages[i], images[i], []string{"ID"}) {
				t.Fatalf("Expected %v, got %v", inputImages[i], images[i])
			}
			if images[i].ID == (ImageID{}) {
				t.Fatal("Expected ID to be set")
			}
		}
		if images[0].ID == images[1].ID {
			t.Fatal("Expected IDs to be different")
		}
	})
}
