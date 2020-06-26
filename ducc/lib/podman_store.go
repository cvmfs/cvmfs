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
	da "github.com/cvmfs/ducc/docker-api"
)

type ImageInfo struct {
	ID	string	`json:"id,omitempty"`
	Names	[]string	`json:"names,omitempty"`
	Layer	string	`json:"layer,omitempty"`
	Created	time.Time	`json:"created,omitempty"`	
}

type LayerInfo struct {
	ID	string	`json:"id,omitempty"`
	Parent	string	`json:"parent,omitempty"`
	Created	time.Time	`json:"created,omitempty"`
	CompressedDiffDigest	string	`json:"compressed-diff-digest,omitempty"`
	CompressedSize	int	`json:"compressed-size,omitempty"`
	UncompressedDigest	string	`json:"diff-digest,omitempty"`
	UncompressedSize	int64	`json:"diff-size,omitempty"`
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
	//LayerInfoMap contains the layer info 
	LayerInfoMap = make(map[string]LayerInfo)
	//LayerMetadata contains the layer info as json objects
	LayerMetadata []LayerInfo
	//ImageMetadata contains the image info as json objects
	ImageMetadata []ImageInfo
)

func calculateId(digest string) string {
	return strings.Split(digest, ":")[1]
}

type ReadCloserBuffer struct {
	*bytes.Buffer
}

func (cb ReadCloserBuffer) Close() (err error) {
	return
} 

func ComputeLayerInfo(layer da.Layer, in io.ReadCloser) (path io.ReadCloser, err error) {
	Log().WithFields(log.Fields{"action": "Computing the layer info"}).Info(layer.Digest)
	hash := sha256.New()

	var forDigest, forPath bytes.Buffer
	n, err := io.Copy(&forDigest, in)
	if err != nil {
		LogE(err).Warning("Error in reading the layer from gzip")
		return
	}
	for n > 0 {
		n, err = io.Copy(&forDigest, in)
		if err != nil {
			LogE(err).Error("Error in reading layer from buffer")
			return
		}
	}
	forPath = forDigest
	path = ReadCloserBuffer{&forPath}
	size, err := io.Copy(hash, &forDigest)
	if err != nil {
		LogE(err).Warning("Error in computing uncompressed layer digest (diffid)")
		return
	}

	diffID := fmt.Sprintf("%x",hash.Sum(nil))
	created := time.Now()

	layerinfo := LayerInfo{
		ID: diffID,
		Created: created,
		CompressedDiffDigest: layer.Digest,
		CompressedSize: layer.Size,
		UncompressedDigest: "sha256:" + diffID,
		UncompressedSize: size,
	}

	LayerInfoMap[layer.Digest] = layerinfo
	LayerMetadata = append(LayerMetadata, layerinfo)

	return 
}

func (img Image) MapParentLayer() (err error) {
	Log().WithFields(log.Fields{"action": "Mapping parent child relation for the image"}).Info(img.GetSimpleName())
	manifest, err := img.GetManifest()
	if err != nil {
		LogE(err).Warn("Error in getting the image manifest")
		return err
	}

	lastLayer := ""
	for _, layer := range manifest.Layers {
		if lastLayer == "" {
			lastLayer = layer.Digest
			continue
		}
		lastlayerinfo := LayerInfoMap[lastLayer]
		currlayerinfo := LayerInfoMap[layer.Digest]
		currlayerinfo.Parent = calculateId(lastlayerinfo.UncompressedDigest)
		LayerInfoMap[layer.Digest] = currlayerinfo
		lastLayer = layer.Digest	
	}
	return nil
}

func(img Image) CreateImageInfo(CVMFSRepo string) (err error) {
	Log().WithFields(log.Fields{"action": "Ingesting image to images.json in podman store"}).Info(img.GetSimpleName())
	manifest, err := img.GetManifest()
	if err != nil {
		LogE(err).Warn("Error in getting the image manifest")
		return err
	}

	id := calculateId(manifest.Config.Digest)
	layerid := manifest.Layers[len(manifest.Layers)-1].Digest
	topLayer := LayerInfoMap[layerid].ID
	creationTime := time.Now()

	imageinfo := ImageInfo{
		ID: id,
		Names: []string{img.GetSimpleName()},
		Layer: topLayer,
		Created: creationTime,
	}

	ImageMetadata = append(ImageMetadata, imageinfo)
	imgInfo, err := json.Marshal(ImageMetadata)

	imageInfoPath := filepath.Join(rootPath, imageMetadataDir, "images.json")
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
	err = IngestIntoCVMFS(CVMFSRepo, imageInfoPath, tmpFile.Name())
	os.RemoveAll(tmpFile.Name())
	if err != nil {
		return err
	}
	return nil
}

func (img Image) CreateLayerInfo(CVMFSRepo string) (err error) {
	Log().WithFields(log.Fields{"action": "Ingesting layer to layers.json in podman store"}).Info(img.GetSimpleName())
	jsonLayerInfo, err := json.Marshal(LayerMetadata)

	layerInfoPath := filepath.Join(rootPath, layerMetadataDir, "layers.json")
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

	//Ingest images.json file
	err = IngestIntoCVMFS(CVMFSRepo, layerInfoPath, tmpFile.Name())
	os.RemoveAll(tmpFile.Name())
	if err != nil {
		return err
	}
	return nil
}

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

	err = img.MapParentLayer()
	if err != nil {
		LogE(err).Warning("Unable to create parent child relation between layers")
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

	return nil
}
