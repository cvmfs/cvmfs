package db

import (
	"database/sql"

	"github.com/opencontainers/go-digest"
)

const manifestSqlFieldsOrdered string = "image_id, file_digest, schema_version, media_type, config_digest, config_media_type, config_size"
const manifestSqlFieldsQs string = "?,?,?,?,?,?,?"
const layerSqlFieldsOrdered string = "manifest_id, layer_number, digest, media_type, size"
const layerSqlFieldsQs string = "?,?,?,?,?"
const layerSqlQueryFields string = "layer_number, digest, media_type, size"

type ManifestBlob struct {
	MediaType string        `json:"mediaType"`
	Size      int           `json:"size"`
	Digest    digest.Digest `json:"digest"`
}

type Manifest struct {
	FileDigest    digest.Digest  `json:"-"` // Not in JSON but used for the database
	SchemaVersion int            `json:"schemaVersion"`
	MediaType     string         `json:"mediaType"`
	Config        ManifestBlob   `json:"config"`
	Layers        []ManifestBlob `json:"layers"`
}

// CreateManifestForImageID stores the provided manifest in the database for the given image ID.
// If a manifest already exists for the image ID, it will be replaced.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func CreateManifestForImageID(tx *sql.Tx, imageID ImageID, manifest Manifest) error {
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

	// Remove the old manifest. This also cascade deletes the layers.
	err := DeleteManifestByImageID(tx, imageID)
	if err != nil {
		return err
	}

	// Create the new manifest
	const manifestStmnt string = "INSERT INTO manifests (" + manifestSqlFieldsOrdered + ") VALUES (" + manifestSqlFieldsQs + ")"
	_, err = tx.Exec(manifestStmnt, imageID, manifest.FileDigest.String(), manifest.SchemaVersion, manifest.MediaType, manifest.Config.Digest, manifest.Config.MediaType, manifest.Config.Size)
	if err != nil {
		return err
	}
	// ...and its layers
	const layerStmnt string = "INSERT INTO manifest_layers (" + layerSqlFieldsOrdered + ") VALUES (" + layerSqlFieldsQs + ")"
	layerPrepStmnt, err := tx.Prepare(layerStmnt)
	if err != nil {
		return err
	}
	defer layerPrepStmnt.Close()
	for i, layer := range manifest.Layers {
		_, err = layerPrepStmnt.Exec(imageID, i, layer.Digest, layer.MediaType, layer.Size)
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

// GetManifestsByImageIDs returns the manifests for the given image IDs.
// Unless all manifests are found, an error is returned.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func GetManifestsByImageIDs(tx *sql.Tx, imageIDs []ImageID) ([]Manifest, error) {
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

	const stmnt string = "SELECT " + manifestSqlFieldsOrdered + " FROM manifests WHERE image_id = ?"
	prepStmnt, err := tx.Prepare(stmnt)
	if err != nil {
		return nil, err
	}
	defer prepStmnt.Close()

	const layerStmnt string = "SELECT " + layerSqlQueryFields + " FROM manifest_layers WHERE manifest_id = ? ORDER BY layer_number ASC"
	layerPrepStmnt, err := tx.Prepare(layerStmnt)
	if err != nil {
		return nil, err
	}
	defer layerPrepStmnt.Close()

	out := make([]Manifest, 0, len(imageIDs))
	for _, imageID := range imageIDs {
		// Get the main manifest
		row := prepStmnt.QueryRow(imageID)
		manifest, err := parseManifestFromRow(row)
		if err != nil {
			return nil, err
		}
		// Get the layers
		manifest.Layers = make([]ManifestBlob, 0)
		rows, err := layerPrepStmnt.Query(imageID)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			layer, err := parseLayerFromRow(rows)
			if err != nil {
				return nil, err
			}
			manifest.Layers = append(manifest.Layers, layer)
		}
		out = append(out, manifest)
	}

	if ownTx {
		err := tx.Commit()
		if err != nil {
			return nil, err
		}
	}

	return out, nil
}

// GetManifestByImageID returns the manifest for the given image ID.
// If no manifest is found, an error is returned.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func GetManifestByImageID(tx *sql.Tx, imageID ImageID) (Manifest, error) {
	manifests, err := GetManifestsByImageIDs(tx, []ImageID{imageID})
	if err != nil {
		return Manifest{}, err
	}
	return manifests[0], nil
}

// DeleteManifestByImageID deletes the manifest for the given image ID.
// This is a no-op if the manifest or image does not exist.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func DeleteManifestByImageID(tx *sql.Tx, imageID ImageID) error {
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

	const stmnt string = "DELETE FROM manifests WHERE image_id = ?"
	_, err := tx.Exec(stmnt, imageID)
	if err != nil {
		return err
	}

	if ownTx {
		err := tx.Commit()
		if err != nil {
			return err
		}
	}
	return nil
}

// GetImagesByLayerDigest returns the images that contain a layer with the given layer digest.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func GetImagesByLayerDigest(tx *sql.Tx, layerDigest digest.Digest) ([]Image, error) {
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

	query := `
    SELECT ` + imageSqlFieldsOrderedPrefixed + `
    FROM manifest_layers
    JOIN manifests ON manifest_layers.manifest_id = manifests.image_id
    JOIN images ON manifests.image_id = images.id
    WHERE manifest_layers.digest = ? 
    `
	rows, err := tx.Query(query, layerDigest.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var images []Image
	for rows.Next() {
		image, err := parseImageFromRow(rows)
		if err != nil {
			return nil, err
		}
		images = append(images, image)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	if ownTx {
		err := tx.Commit()
		if err != nil {
			return nil, err
		}
	}

	return images, nil
}

// parseManifestFromRow parses a manifest from a database row.
// The row must contain the fields in manifestSqlFieldsOrdered, in order.
func parseManifestFromRow(row scannableRow) (Manifest, error) {
	out := Manifest{}
	var fileDigestStr string
	var configDigestStr string

	err := row.Scan(&out.FileDigest, &fileDigestStr, &out.SchemaVersion, &out.MediaType, &configDigestStr, &out.Config.MediaType, &out.Config.Size)
	if err != nil {
		return Manifest{}, err
	}
	out.Config.Digest, err = digest.Parse(configDigestStr)
	if err != nil {
		return Manifest{}, err
	}
	out.FileDigest, err = digest.Parse(fileDigestStr)
	if err != nil {
		return Manifest{}, err
	}

	return out, nil
}

// parseLayerFromRow parses a layer from a database row.
// The row must contain the fields in layerSqlQueryFields, in order.
func parseLayerFromRow(row scannableRow) (ManifestBlob, error) {
	out := ManifestBlob{}
	var digestStr string
	var layer_number int

	err := row.Scan(&layer_number, &digestStr, &out.MediaType, &out.Size)
	if err != nil {
		return ManifestBlob{}, err
	}
	out.Digest, err = digest.Parse(digestStr)
	if err != nil {
		return ManifestBlob{}, err
	}

	return out, nil
}

// ContainsForeignLayers returns true if the given manifest contains foreign layers.
func (m Manifest) ContainsForeignLayers() bool {
	for _, layer := range m.Layers {
		if LayerMediaTypeIsForeign(layer.MediaType) {
			return true
		}
	}
	return false
}

// LayerMediaTypeIsValid returns true if the given layer media type is a valid OCI or Docker layer media type.
func LayerMediaTypeIsValid(layerMediaType string) bool {
	validLayerMediaTypes := []string{
		"application/vnd.oci.image.layer.v1.tar",
		"application/vnd.oci.image.layer.v1.tar+gzip",
		"application/vnd.oci.image.layer.nondistributable.v1.tar",
		"application/vnd.oci.image.layer.nondistributable.v1.tar+gzip",
		"application/vnd.docker.image.rootfs.diff.tar",
		"application/vnd.docker.image.rootfs.diff.tar.gzip",
		"application/vnd.docker.image.rootfs.foreign.diff.tar",
		"application/vnd.docker.image.rootfs.foreign.diff.tar.gzip",
	}
	for _, validLayerMediaType := range validLayerMediaTypes {
		if layerMediaType == validLayerMediaType {
			return true
		}
	}
	return false
}

// LayerMediaTypeIsCompressed returns true if the given layer media type is a gzip compressed layer.
func LayerMediaTypeIsCompressed(layerMediaType string) bool {
	compressedLayerMediaTypes := []string{
		"application/vnd.oci.image.layer.v1.tar+gzip",
		"application/vnd.oci.image.layer.nondistributable.v1.tar+gzip",
		"application/vnd.docker.image.rootfs.diff.tar.gzip",
		"application/vnd.docker.image.rootfs.foreign.diff.tar.gzip",
	}
	for _, compressedLayerMediaType := range compressedLayerMediaTypes {
		if layerMediaType == compressedLayerMediaType {
			return true
		}
	}
	return false
}

// LayerMediaTypeIsForeign returns true if the given layer media type is a foreign layer.
// Oci non-distributable layers are also considered foreign.
func LayerMediaTypeIsForeign(layerMediaType string) bool {
	foreignLayerTypes := []string{
		"application/vnd.oci.image.layer.nondistributable.v1.tar",
		"application/vnd.oci.image.layer.nondistributable.v1.tar+gzip",
		"application/vnd.docker.image.rootfs.foreign.diff.tar.gzip",
		"application/vnd.docker.image.rootfs.foreign.diff.tar",
	}
	for _, foreignLayerType := range foreignLayerTypes {
		if layerMediaType == foreignLayerType {
			return true
		}
	}
	return false
}
