package db

import (
	"database/sql"
	"fmt"
)

// GetImagesByWishIDs takes in a list of wishIDs and returns all images linked to each wish
// in a 2D slice. The first dimension is the wishID, in the same order as the input slice.
// The second dimension is the list of images linked to the wish.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.

func GetImagesByWishIDs(tx *sql.Tx, wishIDs []WishID) ([][]Image, error) {
	ownTx := false
	if tx == nil {
		ownTx = true
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
	}

	const stmnt string = "SELECT " + imageSqlFieldsOrdered + " FROM images JOIN wish_image ON images.id = wish_image.image_id  WHERE wish_image.wish_id = ?"
	prepStmnt, err := tx.Prepare(stmnt)
	if err != nil {
		return nil, err
	}
	defer prepStmnt.Close()

	out := make([][]Image, 0, len(wishIDs))

	for _, wishID := range wishIDs {
		row, err := prepStmnt.Query(wishID)
		if err != nil {
			return nil, err
		}
		images := make([]Image, 0)
		for row.Next() {
			image, err := parseImageFromRow(row)
			if err != nil {
				return nil, err
			}
			images = append(images, image)
		}
		out = append(out, images)
	}

	if ownTx {
		err := tx.Commit()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// GetImagesByWishID returns all images linked to a wish.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func GetImagesByWishID(tx *sql.Tx, wishID WishID) ([]Image, error) {
	ownTx := false
	if tx == nil {
		ownTx = true
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
	}

	stmnt := "SELECT " + imageSqlFieldsOrdered + " FROM images JOIN wish_image ON images.id = wish_image.image_id  WHERE wish_image.wish_id = ?"
	rows, err := tx.Query(stmnt, wishID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	images := make([]Image, 0)
	for rows.Next() {
		image, err := parseImageFromRow(rows)
		if err != nil {
			return nil, err
		}
		images = append(images, image)
	}

	if ownTx {
		err := tx.Commit()
		if err != nil {
			return nil, err
		}
	}
	return images, nil
}

// UpdateImagesForWish takes in a wishID and a slice of images and updates the database accordingly.
// All images in the slice that are not in the database are created and linked to the wish.
// All images that are in the database and in the slice, but not linked to the wish, are linked.
// All images that are in the database but not in the slice are unlinked from the wish.
// Three slices are returned: the created images, the newly linked (updated) images and the unlinked images.
// Unless all operations are successful, an error is returned.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func UpdateImagesForWish(tx *sql.Tx, wishId WishID, images []Image) (created []Image, updated []Image, unlinked []Image, err error) {
	ownTx := false
	if tx == nil {
		ownTx = true
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return nil, nil, nil, err
		}
		defer tx.Rollback()
	}

	// Check that the wish still exists
	_, err = GetWishByID(tx, wishId)
	if err == sql.ErrNoRows {
		return nil, nil, nil, fmt.Errorf("the wish does not exist anymore: %s", err)
	} else if err != nil {
		return nil, nil, nil, err
	}

	imagesToCreateAndLink := make([]Image, 0)
	imagesToLink := make([]Image, 0)

	currentValidIds := make(map[ImageID]struct{})

	// 1: Find which images are new and which are already in the database
	for _, image := range images {
		dbImage, err := GetImageByValue(tx, image)
		if err == sql.ErrNoRows {
			// The image is not in the database, we create it
			imagesToCreateAndLink = append(imagesToCreateAndLink, image)
			continue
		} else if err != nil {
			return nil, nil, nil, err
		}
		// The image exists, we just link it to the wish
		imagesToLink = append(imagesToLink, dbImage)
		currentValidIds[dbImage.ID] = struct{}{}
	}

	// 2. Create images that are not in the database
	created, err = CreateImages(tx, imagesToCreateAndLink) // Populates the "created" output slice
	if err != nil {
		return nil, nil, nil, err
	}
	for _, image := range created {
		currentValidIds[image.ID] = struct{}{}
	}

	// 3. Link both new and existing images to the wish
	linkOperationIDs := make([]ImageID, 0, len(created)+len(imagesToLink))
	for _, image := range created {
		linkOperationIDs = append(linkOperationIDs, image.ID)
	}
	for _, image := range imagesToLink {
		linkOperationIDs = append(linkOperationIDs, image.ID)
	}
	areLinksNew, err := linkImagesToWish(tx, wishId, linkOperationIDs)
	if err != nil {
		return nil, nil, nil, err
	}
	// Populate the "updated" output slice with the existing images. Skip the ones that were already linked to this wish.
	updated = make([]Image, 0)
	for i, newLink := range areLinksNew[len(created):] {
		if newLink {
			updated = append(updated, imagesToLink[i])
		}
	}

	// 4: Unlink images that are no longer referenced by the wish
	unlinked = make([]Image, 0)
	toUnlinkIDs := make([]ImageID, 0)
	linkedImagesInDB, err := GetImagesByWishID(tx, wishId)
	if err != nil {
		return nil, nil, nil, err
	}
	for _, linkedImage := range linkedImagesInDB {
		// If the image is not in the input slice, we unlink it from this wish
		if _, exists := currentValidIds[linkedImage.ID]; !exists {
			unlinked = append(unlinked, linkedImage) // Populates the "unlinked" output slice
			toUnlinkIDs = append(toUnlinkIDs, linkedImage.ID)
		}
	}
	err = unlinkImagesByIdFromWish(tx, wishId, toUnlinkIDs)
	if err != nil {
		return nil, nil, nil, err
	}

	if ownTx {
		err := tx.Commit()
		if err != nil {
			return nil, nil, nil, err
		}
	}

	return created, updated, unlinked, nil
}

// unlinkImagesByIdFromWish removes the link between a wishID and a list of image IDs.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func unlinkImagesByIdFromWish(tx *sql.Tx, wishID WishID, imageIDs []ImageID) error {
	ownTx := false
	if tx == nil {
		ownTx = true
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	const stmnt string = "DELETE FROM wish_image WHERE wish_id = ? AND image_id = ?"
	prepStmnt, err := tx.Prepare(stmnt)
	if err != nil {
		return err
	}
	defer prepStmnt.Close()

	for _, imageID := range imageIDs {
		_, err := prepStmnt.Exec(wishID, imageID)
		if err != nil {
			return err
		}
	}

	if ownTx {
		err := tx.Commit()
		if err != nil {
			return err
		}
	}
	return nil
}

// linkImageToWish takes in a wishID and an image ID and links them to the wish.
// It returns a boolean indicating whether the link was just created (true) or already existed (false).
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func linkImageToWish(tx *sql.Tx, wishID WishID, imageId ImageID) (updated bool, err error) {
	updatedArr, err := linkImagesToWish(tx, wishID, []ImageID{imageId})
	if err != nil {
		return false, err
	}
	return updatedArr[0], nil
}

// linkImagesToWish takes in a wishID and a slice of image IDs and links them to the wish.
// It returns a slice of booleans indicating whether the link was just created (true) or already existed (false).
// Unless all operations are successful, an error is returned.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func linkImagesToWish(tx *sql.Tx, wishID WishID, imageIDs []ImageID) (updated []bool, err error) {
	ownTx := false
	if tx == nil {
		ownTx = true
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
	}

	insertStmnt := "INSERT INTO wish_image (wish_id, image_id) VALUES (?, ?)"
	insertPrepStmnt, err := tx.Prepare(insertStmnt)
	if err != nil {
		return nil, err
	}
	defer insertPrepStmnt.Close()

	checkStmnt := "SELECT COUNT(*) FROM wish_image WHERE wish_id = ? AND image_id = ?"
	checkPrepStmnt, err := tx.Prepare(checkStmnt)
	if err != nil {
		return nil, err
	}
	defer checkPrepStmnt.Close()

	updated = make([]bool, 0, len(imageIDs))
	for _, imageID := range imageIDs {
		var count int
		err := checkPrepStmnt.QueryRow(wishID, imageID).Scan(&count)
		if err != nil {
			return nil, err
		}
		if count == 1 {
			// The link already exists
			updated = append(updated, false)
			continue
		}
		// The link does not exist, we create it
		_, err = insertPrepStmnt.Exec(wishID, imageID)
		if err != nil {
			return nil, err
		}
		updated = append(updated, true)
	}

	if ownTx {
		err := tx.Commit()
		if err != nil {
			return nil, err
		}
	}

	return updated, nil
}

// GetWishesByImageID returns all wishes linked to an image.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func GetWishesByImageID(tx *sql.Tx, imageID ImageID) ([]Wish, error) {
	wishes, err := GetWishesByImageIDs(tx, []ImageID{imageID})
	if err != nil {
		return nil, err
	}
	return wishes[0], nil
}

// GetWishesByImageIDs returns all wishes linked to a list of images.
// It returns a 2D slice of wishes. The first dimension corresponds to the image IDs, in the same order
// as the input slice. The second dimension corresponds to the wishes linked to each image.
// Unless all images are found, sql.NoRows is returned.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.

func GetWishesByImageIDs(tx *sql.Tx, imageIDs []ImageID) ([][]Wish, error) {
	ownTx := false
	if tx == nil {
		ownTx = true
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
	}

	const stmnt string = "SELECT " + wishSqlFieldsOrdered + " FROM wishes JOIN wish_image ON wishes.id = wish_image.wish_id WHERE wish_image.image_id = ?"
	prepStmnt, err := tx.Prepare(stmnt)
	if err != nil {
		return nil, err
	}
	defer prepStmnt.Close()

	out := make([][]Wish, 0, len(imageIDs))
	for _, imageID := range imageIDs {
		row, err := prepStmnt.Query(imageID)
		if err != nil {
			return nil, err
		}
		wishes := make([]Wish, 0)
		for row.Next() {
			wish, err := parseWishFromRow(row)
			if err != nil {
				return nil, err
			}
			wishes = append(wishes, wish)
		}
		out = append(out, wishes)
	}

	if ownTx {
		err := tx.Commit()
		if err != nil {
			return nil, err
		}
	}

	return out, nil
}
