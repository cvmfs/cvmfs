package products

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/cvmfs/ducc/db"
	"github.com/cvmfs/ducc/registry"
)

func CheckFlatImage(cvmfsRepo string, image db.Image, manifest registry.ManifestWithBytesAndDigest) (bool, error) {
	// Generate the flat chain
	flatChain := GenerateChainFromManifest(manifest.Manifest)

	// Check that all chain links are present
	for _, chainLink := range flatChain {
		chainLinkPath := filepath.Join("/cvmfs", cvmfsRepo, ".chains", chainLink.ChainDigest.Encoded()[:2], chainLink.ChainDigest.Encoded())
		_, err := os.Stat(chainLinkPath)
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		} else if err != nil {
			return false, err
		}
	}

	// Check that the .flat directory is present
	flatDirPath := filepath.Join("/cvmfs", cvmfsRepo, ".flat", manifest.ManifestDigest.Encoded()[:2], manifest.ManifestDigest.Encoded())
	flatDirInfo, err := os.Stat(flatDirPath)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else if err != nil {
		return false, err
	}

	// Check that the public symlink is present
	publicSymlinkPath := filepath.Join("/cvmfs", cvmfsRepo, image.GetSimpleName())
	publicSymlinkInfo, err := os.Stat(publicSymlinkPath)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else if err != nil {
		return false, err
	}

	// Check that the public symlink points to the correct directory
	if !os.SameFile(publicSymlinkInfo, flatDirInfo) {
		return false, nil
	}

	return true, nil
}
