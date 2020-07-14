//This library creates a Podman image store.
//Podman image store has the following directory structure:

//	podmanStore
//	+--	overlay
//	|	+-- $(layerid)
//	|	|	+-- diff dir
//	|	|	+--	link file
//	|	+-- l
//	+-- overlay-images
//	|	+-- $(imageid)
//	|	|	+-- config file
//	|	|	+-- manifest file
//	|	+-- images.json
//	|	+-- images.lock
//	+-- overlay-layers
//	|	+--	layers.json
//	|	+-- layers.lock

package lib

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"time"

	log "github.com/sirupsen/logrus"
)

//podman related info stored in image struct.
type PodmanInfo struct {
	LayerReaderMap map[string]ReadCloserBuffer
	LayerDigestMap map[string]string
	LayerIdMap     map[string]string
}

//struct for entries in images.json
type ImageInfo struct {
	ID      string    `json:"id,omitempty"`
	Names   []string  `json:"names,omitempty"`
	Layer   string    `json:"layer,omitempty"`
	Created time.Time `json:"created,omitempty"`
}

//struct for entries in layers.json
type LayerInfo struct {
	ID                   string    `json:"id,omitempty"`
	Parent               string    `json:"parent,omitempty"`
	Created              time.Time `json:"created,omitempty"`
	CompressedDiffDigest string    `json:"compressed-diff-digest,omitempty"`
	CompressedSize       int       `json:"compressed-size,omitempty"`
	UncompressedDigest   string    `json:"diff-digest,omitempty"`
	UncompressedSize     int64     `json:"diff-size,omitempty"`
}

var (
	//Podman Additional Image Store inside unpacked.cern.ch
	rootPath = "podmanStore"
	//rootfsDir contains the exploded rootfs of images.
	rootfsDir = "overlay"
	//imageMetadataDir contains the metadata, config and manifest file of images.
	imageMetadataDir = "overlay-images"
	//layerMetadataDir contains the metadata of layers
	layerMetadataDir = "overlay-layers"
)

//Computes uncompressed digest and size for the given layer
func (img *Image) ComputeLayerInfo(digest, parent string, layersize int) (layerinfo *LayerInfo, err error) {
	hash := sha256.New()
	podmaninfo := img.GetPodmanInfo()
	in := podmaninfo.LayerReaderMap[digest]
	size, err := io.Copy(hash, in)
	if err != nil {
		LogE(err).Warning("Error in computing uncompressed layer digest (diffid)")
		return layerinfo, err
	}
	diffID := fmt.Sprintf("%x", hash.Sum(nil))
	created := time.Now()
	layerinfo = &LayerInfo{
		ID:                   diffID,
		Created:              created,
		Parent:               parent,
		CompressedDiffDigest: digest,
		CompressedSize:       layersize,
		UncompressedDigest:   "sha256:" + diffID,
		UncompressedSize:     size,
	}
	podmaninfo.LayerDigestMap[digest] = diffID
	return
}

//creates images.json and layers.json file in podman store
func (img Image) IngestImageInfo(CVMFSRepo string) (err error) {
	manifest, err := img.GetManifest()
	if err != nil {
		LogE(err).Warn("Error in getting the image manifest")
		return
	}
	//create layers.json file
	Log().WithFields(log.Fields{"action": "Ingesting layers.json in podman store"}).Info(img.GetSimpleName())
	layersdata := []LayerInfo{}
	layerInfoPath := filepath.Join("/", "cvmfs", CVMFSRepo, rootPath, layerMetadataDir, "layers.json")

	//check if layers.json already exist and append to data
	if _, err := os.Stat(layerInfoPath); err == nil {
		file, err := ioutil.ReadFile(layerInfoPath)
		if err != nil {
			LogE(err).Error("Error in reading layers.json file")
			return err
		}
		json.Unmarshal(file, &layersdata)
	}

	parent := ""
	topLayer := ""
	for _, layer := range manifest.Layers {
		layerinfo, err := img.ComputeLayerInfo(layer.Digest, parent, layer.Size)
		if err != nil {
			LogE(err).Error("Error in computing layer info")
			return err
		}
		if parent == "" {
			topLayer = layerinfo.ID
		}
		parent = layerinfo.ID
		layersdata = append(layersdata, *layerinfo)
	}

	jsonLayerInfo, err := json.MarshalIndent(layersdata, "", " ")
	if err != nil {
		LogE(err).Error("Error in marshaling json data for layers.json")
		return err
	}

	err = writeDataToCvmfs(CVMFSRepo, TrimCVMFSRepoPrefix(layerInfoPath), jsonLayerInfo)
	if err != nil {
		LogE(err).Error("Error in writing layers.json file")
		return err
	}

	//create images.json file
	Log().WithFields(log.Fields{"action": "Ingesting images.json in podman store"}).Info(img.GetSimpleName())
	imagedata := []ImageInfo{}
	imageInfoPath := filepath.Join("/", "cvmfs", CVMFSRepo, rootPath, imageMetadataDir, "images.json")

	//check if images.json already exist and append to data
	if _, err := os.Stat(imageInfoPath); err == nil {
		file, err := ioutil.ReadFile(imageInfoPath)
		if err != nil {
			LogE(err).Error("Error in reading layers.json file")
			return err
		}
		json.Unmarshal(file, &imagedata)
	}

	id := calculateId(manifest.Config.Digest)
	creationTime := time.Now()
	imageinfo := &ImageInfo{
		ID:      id,
		Names:   []string{img.GetSimpleName()},
		Layer:   topLayer,
		Created: creationTime,
	}

	imagedata = append(imagedata, *imageinfo)
	imgInfo, err := json.MarshalIndent(imagedata, "", " ")
	if err != nil {
		LogE(err).Error("Error in marshaling json data for images.json")
		return err
	}

	err = writeDataToCvmfs(CVMFSRepo, TrimCVMFSRepoPrefix(imageInfoPath), imgInfo)
	if err != nil {
		LogE(err).Error("Error in writing images.json")
		return err
	}
	return nil
}

//Ingest the exploded rootfs in podman store.
func (img Image) IngestRootfsIntoPodmanStore(CVMFSRepo, subDirInsideRepo string) (err error) {
	Log().WithFields(log.Fields{"action": "Ingesting layer rootfs into podman store for the image"}).Info(img.GetSimpleName())
	manifest, err := img.GetManifest()
	if err != nil {
		LogE(err).Warn("Error in getting the image manifest")
		return err
	}
	podmaninfo := img.GetPodmanInfo()
	for _, layer := range manifest.Layers {
		layerid := calculateId(layer.Digest)
		layerdir := podmaninfo.LayerDigestMap[layer.Digest]

		//symlinkPath will contain the rootfs of the corresponding layer in podman store.
		symlinkPath := filepath.Join(rootPath, rootfsDir, layerdir, "diff")
		targetPath := filepath.Join(subDirInsideRepo, layerid[:2], layerid, "layerfs")

		err = CreateSymlinkIntoCVMFS(CVMFSRepo, symlinkPath, targetPath)
		if err != nil {
			LogE(err).Error("Error in creating the symlink for the diff dir")
			return err
		}
	}
	return nil
}

//Create the link dir and link file for exploded rootfs.
func (img Image) CreateLinkDir(CVMFSRepo, subDirInsideRepo string) (err error) {
	Log().WithFields(log.Fields{"action": "Creating Link files for layer rootfs for the image"}).Info(img.GetSimpleName())
	manifest, err := img.GetManifest()
	if err != nil {
		LogE(err).Warn("Error in getting the image manifest")
		return err
	}
	podmaninfo := img.GetPodmanInfo()
	for _, layer := range manifest.Layers {
		layerdir := podmaninfo.LayerDigestMap[layer.Digest]

		//generate the link id
		lid := generateID(26)
		podmaninfo.LayerIdMap[layer.Digest] = "l/" + lid

		//Create link dir
		symlinkPath := filepath.Join(rootPath, rootfsDir, "l", lid)
		targetPath := filepath.Join(rootPath, rootfsDir, layerdir, "diff")

		err = CreateSymlinkIntoCVMFS(CVMFSRepo, symlinkPath, targetPath)
		if err != nil {
			LogE(err).Error("Error in creating the symlink for the Link dir")
			return err
		}

		linkPath := filepath.Join(rootPath, "overlay", layerdir, "link")
		err = writeDataToCvmfs(CVMFSRepo, linkPath, []byte(lid))
		if err != nil {
			LogE(err).Error("Error in writing link id to podman store")
			return err
		}
	}
	return nil
}

//Create the lower files for diff dirs to be used by podman.
func (img Image) CreateLowerFiles(CVMFSRepo string) (err error) {
	Log().WithFields(log.Fields{"action": "Creating Lower files for diff dirs in podman store"}).Info(img.GetSimpleName())
	manifest, err := img.GetManifest()
	if err != nil {
		LogE(err).Warn("Error in getting the image manifest")
		return err
	}

	lastLowerData := ""
	lastDigest := ""
	podmaninfo := img.GetPodmanInfo()
	for _, layer := range manifest.Layers {
		if lastDigest == "" {
			lastDigest = layer.Digest
			continue
		}

		lowerdata := podmaninfo.LayerIdMap[lastDigest]
		if lastLowerData != "" {
			lowerdata = lowerdata + ":" + lastLowerData
		}
		layerdir := podmaninfo.LayerDigestMap[layer.Digest]
		lowerPath := filepath.Join(rootPath, rootfsDir, layerdir, "lower")
		err = writeDataToCvmfs(CVMFSRepo, lowerPath, []byte(lowerdata))
		if err != nil {
			LogE(err).Warn("Error in writing lower files")
			return err
		}
		lastLowerData = lowerdata
		lastDigest = layer.Digest
	}
	return nil
}

//Ingest the image config file in podman store.
func (img Image) IngestConfigFile(CVMFSRepo string) (err error) {
	Log().WithFields(log.Fields{"action": "Creating config file for the image in podman store"}).Info(img.GetSimpleName())
	manifest, err := img.GetManifest()
	if err != nil {
		LogE(err).Warn("Error in getting the image manifest")
		return err
	}

	user := img.User
	pass, err := GetPassword()
	if err != nil {
		LogE(err).Warning("Unable to get the credential for downloading the configuration blog, trying anonymously")
		user = ""
		pass = ""
	}

	configUrl := fmt.Sprintf("%s://%s/v2/%s/blobs/%s",
		img.Scheme, img.Registry, img.Repository, manifest.Config.Digest)

	token, err := firstRequestForAuth(configUrl, user, pass)
	if err != nil {
		LogE(err).Warning("Unable to retrieve the token for downloading config file")
		return err
	}

	client := &http.Client{}
	req, err := http.NewRequest("GET", configUrl, nil)
	if err != nil {
		LogE(err).Warning("Unable to create a request for getting config file.")
		return err
	}
	req.Header.Set("Authorization", token)
	req.Header.Set("Accept", "application/vnd.docker.distribution.manifest.v2+json")

	resp, err := client.Do(req)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		LogE(err).Warning("Error in reading the body from the configuration")
		return err
	}

	//generate config file path to ingest above temp dir.
	fname, err := generateConfigFileName(manifest.Config.Digest)
	if err != nil {
		LogE(err).Warning("Error in generating config file name")
		return err
	}

	imageID := calculateId(manifest.Config.Digest)
	configFilePath := filepath.Join(rootPath, imageMetadataDir, imageID, fname)

	err = writeDataToCvmfs(CVMFSRepo, configFilePath, []byte(body))
	if err != nil {
		LogE(err).Warning("Error in writing config file")
		return err
	}
	return nil
}

//Ingest the image manifest in podman store.
func (img Image) IngestImageManifest(CVMFSRepo string) (err error) {
	Log().WithFields(log.Fields{"action": "Creating manifest file for the image in podman store"}).Info(img.GetSimpleName())
	manifest, err := img.GetManifest()
	if err != nil {
		LogE(err).Warn("Error in getting the image manifest")
		return err
	}
	imageID := calculateId(manifest.Config.Digest)

	symlinkPath := filepath.Join(rootPath, imageMetadataDir, imageID, "manifest")
	targetPath := filepath.Join(".metadata", img.Registry, img.Repository+img.GetReference(), "manifest.json")

	err = CreateSymlinkIntoCVMFS(CVMFSRepo, symlinkPath, targetPath)
	if err != nil {
		LogE(err).Error("Error in creating the symlink for manifest.json")
		return err
	}
	return nil
}

//Create images.lock and layers.lock file to be used by podman.
//Libpod expects these files to be present in its image stores.
func (img Image) CreateLockFiles(CVMFSRepo string) (err error) {
	Log().WithFields(log.Fields{"action": "Creating lock file for the image"}).Info(img.GetSimpleName())
	layerlockpath := filepath.Join("/cvmfs", CVMFSRepo, rootPath, layerMetadataDir, "layers.lock")
	imagelockpath := filepath.Join("/cvmfs", CVMFSRepo, rootPath, imageMetadataDir, "images.lock")
	for _, file := range []string{layerlockpath, imagelockpath} {
		if _, err := os.Stat(file); os.IsNotExist(err) {
			tmpFile, err := ioutil.TempFile("", "lock")
			tmpFile.Close()
			if err != nil {
				return err
			}
			err = IngestIntoCVMFS(CVMFSRepo, TrimCVMFSRepoPrefix(file), tmpFile.Name())
			os.RemoveAll(tmpFile.Name())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

//Ingest all the necessary files and dir in podmanStore dir.
func (img Image) CreatePodmanImageStore(CVMFSRepo, subDirInsideRepo string) (err error) {
	Log().WithFields(log.Fields{"action": "Ingest the image into podman store"}).Info(img.GetSimpleName())
	createCatalogIntoDirs := []string{rootPath, filepath.Join(rootPath, rootfsDir), filepath.Join(rootPath, imageMetadataDir), filepath.Join(rootPath, layerMetadataDir)}
	for _, dir := range createCatalogIntoDirs {
		err = CreateCatalogIntoDir(CVMFSRepo, dir)
		if err != nil {
			LogE(err).WithFields(log.Fields{
				"directory": dir}).Error(
				"Impossible to create subcatalog in the directory.")
		}
	}

	err = img.IngestImageInfo(CVMFSRepo)
	if err != nil {
		LogE(err).Error("Unable to create images.json and layers.json file in podman store")
		return err
	}

	err = img.CreateLockFiles(CVMFSRepo)
	if err != nil {
		LogE(err).Error("Unable to create layers lock file in podman store")
		return err
	}

	err = img.IngestConfigFile(CVMFSRepo)
	if err != nil {
		LogE(err).Error("Unable to create config file in podman store")
		return err
	}

	err = img.IngestImageManifest(CVMFSRepo)
	if err != nil {
		LogE(err).Error("Unable to create manifest file in podman store")
		return err
	}

	err = img.IngestRootfsIntoPodmanStore(CVMFSRepo, subDirInsideRepo)
	if err != nil {
		LogE(err).Error("Error ingesting rootfs into podman image store")
		return err
	}

	err = img.CreateLinkDir(CVMFSRepo, subDirInsideRepo)
	if err != nil {
		LogE(err).Error("Unable to create the link dir in podman store")
		return err
	}

	err = img.CreateLowerFiles(CVMFSRepo)
	if err != nil {
		LogE(err).Error("Unable to create the lower files in podman store")
		return err
	}

	return nil
}
