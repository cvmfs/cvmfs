package products

import (
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/cvmfs/ducc/registry"
	"github.com/opencontainers/go-digest"
)

func CheckLayers(cvmfsRepo string, manifest registry.ManifestWithBytesAndDigest) (bool, error) {
	imageDirOK, err := CheckImageDirectory(cvmfsRepo, manifest)
	if err != nil {
		return false, err
	}
	if !imageDirOK {
		return false, nil
	}

	for _, layer := range manifest.Manifest.Layers {
		layerOK, err := CheckIndividualLayer(cvmfsRepo, manifest, layer.Digest)
		if err != nil {
			return false, err
		}
		if !layerOK {
			return false, nil
		}
	}
	return true, nil
}

func CheckImageDirectory(cvmfsRepo string, manifest registry.ManifestWithBytesAndDigest) (bool, error) {
	// Check that the directory in .image is present
	imageDir := filepath.Join("/cvmfs", cvmfsRepo, ".images", manifest.ManifestDigest.Encoded()[:2], manifest.ManifestDigest.Encoded())
	imagedirInfo, err := os.Stat(imageDir)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	if !imagedirInfo.IsDir() {
		return false, errors.New("image directory is not a directory")
	}

	// Check that the manifest is present and up to date
	existingManifestFile, err := os.Open(filepath.Join(imageDir, "manifest.json"))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	existingManifestBytes, err := io.ReadAll(existingManifestFile)
	if err != nil {
		return false, err
	}
	if digest.SHA256.FromBytes(existingManifestBytes) != manifest.ManifestDigest {
		// Stored manifest is not up to date
		return false, nil
	}

	// Check the config file
	existingConfigFile, err := os.Open(filepath.Join(imageDir, "config.json"))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	existingConfigBytes, err := io.ReadAll(existingConfigFile)
	if err != nil {
		return false, err
	}
	if digest.SHA256.FromBytes(existingConfigBytes) != manifest.Manifest.Config.Digest {
		// Stored config is not up to date
		return false, nil
	}
	return true, nil
}

func CheckIndividualLayer(cvmfsRepo string, manifest registry.ManifestWithBytesAndDigest, compressedLayerDigest digest.Digest) (bool, error) {
	layerPath := filepath.Join("/cvmfs", cvmfsRepo, ".layers", compressedLayerDigest.Encoded()[:2], compressedLayerDigest.Encoded())
	// Check that the layer is present and up to date
	layerInfo, err := os.Stat(layerPath)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !layerInfo.IsDir() {
		return false, errors.New("layer is not a directory")
	}

	// TODO: Possibly perform more checks verifying the content and metadata of the layer
	return true, nil
}
