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
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

//struct for entries in images.json
type ImageInfo struct {
	ID	string	`json:"id,omitempty"`
	Names	[]string	`json:"names,omitempty"`
	Layer	string	`json:"layer,omitempty"`
	Created	time.Time	`json:"created,omitempty"`	
}
//struct for entries in layers.json
type LayerInfo struct {
	ID	string	`json:"id,omitempty"`
	Parent	string	`json:"parent,omitempty"`
	Created	time.Time	`json:"created,omitempty"`
	CompressedDiffDigest	string	`json:"compressed-diff-digest,omitempty"`
	CompressedSize	int	`json:"compressed-size,omitempty"`
	UncompressedDigest	string	`json:"diff-digest,omitempty"`
	UncompressedSize	int64	`json:"diff-size,omitempty"`
}

type ReadCloserBuffer struct {
	*bytes.Buffer
}
func (cb ReadCloserBuffer) Close() (err error) {
	return
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
	//LayerLinkIdMap contains the link id of rootfs
	LayerLinkId []string
	//LayerReader stores the io.Reader containing layer content
	LayerReader = make(map[string]ReadCloserBuffer)
	//LayerInfoMap contains the layer info 
	LayerInfoMap = make(map[string]LayerInfo)
)

//Computes uncompressed digest and size for the given layer
func (img Image) ComputeLayerInfo() (err error) {
	manifest, err := img.GetManifest()
	if err != nil {
		LogE(err).Warn("Error in getting the image manifest")
		return
	}
	
	lastLayer := ""
	for _, layer := range manifest.Layers {
		Log().WithFields(log.Fields{"action": "Computing the layer info"}).Info(layer.Digest)
		hash := sha256.New()
		in := LayerReader[layer.Digest]
		size, err := io.Copy(hash, in)
		if err != nil {
			LogE(err).Warning("Error in computing uncompressed layer digest (diffid)")
			return err
		}
		diffID := fmt.Sprintf("%x",hash.Sum(nil))
		parent := ""
		if lastLayer != "" {
			lastlayerinfo := LayerInfoMap[lastLayer]
			parent = lastlayerinfo.ID
		}
		created := time.Now()
		layerinfo := &LayerInfo{
			ID: diffID,
			Created: created,
			Parent: parent,
			CompressedDiffDigest: layer.Digest,
			CompressedSize: layer.Size,
			UncompressedDigest: "sha256:" + diffID,
			UncompressedSize: size,
		}
		LayerInfoMap[layer.Digest] = *layerinfo
		lastLayer = layer.Digest
	}
	return
}

//creates images.json file in podman store
func(img Image) CreateImageInfo(CVMFSRepo string) (err error) {
	Log().WithFields(log.Fields{"action": "Ingesting images.json in podman store"}).Info(img.GetSimpleName())

	data := []ImageInfo{}

	imageInfoPath := filepath.Join("/", "cvmfs", CVMFSRepo, rootPath, imageMetadataDir, "images.json")
	if _, err := os.Stat(imageInfoPath); err == nil {
		file, err := ioutil.ReadFile(imageInfoPath)
		if err != nil {
			LogE(err).Error("Error in reading layers.json file")
			return err
		}
		json.Unmarshal(file, &data)
	}

	manifest, err := img.GetManifest()
	if err != nil {
		LogE(err).Warn("Error in getting the image manifest")
		return err
	}

	id := calculateId(manifest.Config.Digest)
	layerid := manifest.Layers[len(manifest.Layers)-1].Digest
	topLayer := LayerInfoMap[layerid].ID
	creationTime := time.Now()

	imageinfo := &ImageInfo{
		ID: id,
		Names: []string{img.GetSimpleName()},
		Layer: topLayer,
		Created: creationTime,
	}

	data = append(data, *imageinfo)
	imgInfo, err := json.MarshalIndent(data, "", " ")
	tmpFile, err := ioutil.TempFile("", "images_json")
	if err != nil {
		LogE(err).Error("Error in creating temporary images.json file")
		return err
	}
	err = ioutil.WriteFile(tmpFile.Name(), imgInfo, 0644)
	tmpFile.Close()
	if err != nil {
		LogE(err).Error("Error in writing to the images.json file")
		return err
	}

	//Ingest images.json file
	err = IngestIntoCVMFS(CVMFSRepo, TrimCVMFSRepoPrefix(imageInfoPath), tmpFile.Name())
	os.RemoveAll(tmpFile.Name())
	if err != nil {
		return err
	}
	return nil
}

//Creates layers.json file in podman store.
func (img Image) CreateLayerInfo(CVMFSRepo string) (err error) {
	Log().WithFields(log.Fields{"action": "Ingesting layer to layers.json in podman store"}).Info(img.GetSimpleName())

	data := []LayerInfo{}

	layerInfoPath := filepath.Join("/", "cvmfs", CVMFSRepo, rootPath, layerMetadataDir, "layers.json")
	if _, err := os.Stat(layerInfoPath); err == nil {
		file, err := ioutil.ReadFile(layerInfoPath)
		if err != nil {
			LogE(err).Error("Error in reading layers.json file")
			return err
		}
		json.Unmarshal(file, &data)
	}
	manifest, err := img.GetManifest()
	if err != nil {
		LogE(err).Warn("Error in getting the image manifest")
		return
	}
	err = img.ComputeLayerInfo()
	if err != nil {
		LogE(err).Error("Error in computing layer info")
		return err
	}
	for _, layer := range manifest.Layers {
		data = append(data, LayerInfoMap[layer.Digest])
	}

	jsonLayerInfo, err := json.MarshalIndent(data, "", " ")
	if err != nil {
		LogE(err).Error("Error in marshaling json data for layers.json")
		return err
	}
	tmpFile, err := ioutil.TempFile("", "layers_json")
	if err != nil {
		LogE(err).Error("Error in creating temporary layers.json file")
		return err
	}
	err = ioutil.WriteFile(tmpFile.Name(), jsonLayerInfo, 0644)
	tmpFile.Close()
	if err != nil {
		LogE(err).Error("Error in writing to the layers.json file")
		return err
	}

	//Ingest layers.json file
	err = IngestIntoCVMFS(CVMFSRepo, TrimCVMFSRepoPrefix(layerInfoPath), tmpFile.Name())
	os.RemoveAll(tmpFile.Name())
	if err != nil {
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

	for _, layer := range manifest.Layers {
		layerid := calculateId(layer.Digest)
		layerdir := LayerInfoMap[layer.Digest].ID

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

	LayerLinkId = nil
	for _, layer := range manifest.Layers {
		layerdir := LayerInfoMap[layer.Digest].ID

		//generate the link id
		lid := generateID(26)
		LayerLinkId = append([]string{"l/"+lid}, LayerLinkId...)

		//Create link dir
		symlinkPath := filepath.Join(rootPath, rootfsDir, "l", lid)
		targetPath := filepath.Join(rootPath, rootfsDir, layerdir, "diff")
		
		err = CreateSymlinkIntoCVMFS(CVMFSRepo, symlinkPath, targetPath)
		if err != nil {
			LogE(err).Error("Error in creating the symlink for the Link dir")
			return err
		}

		//Create link file
		tmpFile, err := ioutil.TempFile("", "linkfile")
		if err != nil {
			return err
		}

		err = ioutil.WriteFile(tmpFile.Name(), []byte(lid), 0644)
		tmpFile.Close()
		if err != nil {
			LogE(err).Error("Error in writing to the link file")
			return err
		}

		//ingest the link file
		linkPath := filepath.Join(rootPath,"overlay",layerdir,"link")
		err = IngestIntoCVMFS(CVMFSRepo, linkPath, tmpFile.Name())
		os.RemoveAll(tmpFile.Name())
		if err != nil {
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

	LayersCount := len(manifest.Layers)
	for i, layer := range manifest.Layers {
		if i==0 {
			continue
		}

		lowerdata := strings.Join(LayerLinkId[LayersCount-i:], ":")
		tmpFile, err := ioutil.TempFile("", "lowerfile")
		if err != nil {
			return err
		}

		err = ioutil.WriteFile(tmpFile.Name(), []byte(lowerdata), 0644)
		tmpFile.Close()
		if err != nil {
			LogE(err).Error("Error in writing to the lower file")
			return err
		}

		//ingest the link file
		layerdir := calculateId(LayerInfoMap[layer.Digest].UncompressedDigest)
		lowerPath := filepath.Join(rootPath, rootfsDir, layerdir, "lower")
		err = IngestIntoCVMFS(CVMFSRepo, lowerPath, tmpFile.Name())
		os.RemoveAll(tmpFile.Name())
		if err != nil {
			return err
		}
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

	//write configuration to a temp file
	tmpFile, err := ioutil.TempFile("", "configFile")
	err = ioutil.WriteFile(tmpFile.Name(), []byte(body), 0644)
	tmpFile.Close()
	if err != nil {
		LogE(err).Error("Error in writing to the config file")
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

	//Ingest config file
	err = IngestIntoCVMFS(CVMFSRepo, configFilePath, tmpFile.Name())
	os.RemoveAll(tmpFile.Name())
	if err != nil {
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
	targetPath := filepath.Join(".metadata", img.Registry, img.Repository + img.GetReference(), "manifest.json")

	err = CreateSymlinkIntoCVMFS(CVMFSRepo, symlinkPath, targetPath)
	if err != nil {
		LogE(err).Error("Error in creating the symlink for manifest.json")
		return err
	}
	return nil
}

//Create images.lock and layers.lock file to be used by podman.
//Libpod expects these files to be present in its image stores.
func (img Image) CreateLockFiles(CVMFSRepo, fpath string) (err error) {
	Log().WithFields(log.Fields{"action": "Creating lock file for the image"}).Info(img.GetSimpleName())
	lockFilePath := filepath.Join("/cvmfs", CVMFSRepo, fpath)
	if _, err := os.Stat(lockFilePath); os.IsNotExist(err) {
		tmpFile, err := ioutil.TempFile("", "lock")
		tmpFile.Close()
		if err != nil {
			return err
		}
		err = IngestIntoCVMFS(CVMFSRepo, TrimCVMFSRepoPrefix(lockFilePath), tmpFile.Name())
		os.RemoveAll(tmpFile.Name())
		if err != nil {
			return err
		}
	}
	return nil
}

//Ingest all the necessary files and dir in podmanStore dir.
func (img Image) CreatePodmanImageStore(CVMFSRepo, subDirInsideRepo string) (err error) {
	Log().WithFields(log.Fields{"action": "Ingest the image into podman store"}).Info(img.GetSimpleName())
	createCatalogIntoDirs := []string{rootPath, filepath.Join(rootPath,rootfsDir), filepath.Join(rootPath,imageMetadataDir), filepath.Join(rootPath,layerMetadataDir)}
	for _, dir := range createCatalogIntoDirs {	
		err = CreateCatalogIntoDir(CVMFSRepo, dir)
		if err != nil {
			LogE(err).WithFields(log.Fields{
				"directory": dir}).Error(
				"Impossible to create subcatalog in the directory.")
		}
	}

	err = img.CreateLayerInfo(CVMFSRepo)
	if err != nil {
		LogE(err).Error("Unable to create layers.json file in podman store")
		return err
	}

	layerlockpath := filepath.Join(rootPath, layerMetadataDir, "layers.lock")
	err = img.CreateLockFiles(CVMFSRepo, layerlockpath)
	if err != nil {
		LogE(err).Error("Unable to create layers lock file in podman store")
		return err
	}

	err = img.CreateImageInfo(CVMFSRepo)
	if err != nil {
		LogE(err).Error("Unable to create images.json file in podman store")
		return err
	}

	imagelockpath := filepath.Join(rootPath, imageMetadataDir, "images.lock")
	err = img.CreateLockFiles(CVMFSRepo, imagelockpath)
	if err != nil {
		LogE(err).Error("Unable to create images lock file in podman store")
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
