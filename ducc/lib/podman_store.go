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

// For more information, have a look at this document:
// https://docs.google.com/document/d/1uP_K6T5tB3qxbN4-S-tRdUkiFggv7Skw3MXZRgWYjS0/edit?usp=sharing

package lib

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	cvmfs "github.com/cvmfs/ducc/cvmfs"
	l "github.com/cvmfs/ducc/log"
	notification "github.com/cvmfs/ducc/notification"
	log "github.com/sirupsen/logrus"
)

// struct for entries in images.json
type ImageInfo struct {
	ID      string    `json:"id,omitempty"`
	Names   []string  `json:"names,omitempty"`
	Layer   string    `json:"layer,omitempty"`
	Created time.Time `json:"created,omitempty"`
}

// struct for entries in layers.json
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

// creates layers.json file in podmanStore.
func (img *Image) PublishLayerInfo(CVMFSRepo string, digestMap map[string]string) (err error) {
	manifest, err := img.GetManifest()
	if err != nil {
		l.LogE(err).Warn("Error in getting the image manifest")
		return
	}
	//create layers.json file
	l.Log().WithFields(log.Fields{"action": "Ingesting layers.json in podman store"}).Info(img.GetSimpleName())

	layersdata := []LayerInfo{}
	layerInfoPath := filepath.Join("/", "cvmfs", CVMFSRepo, rootPath, layerMetadataDir, "layers.json")
	//check if layers.json already exist and append to data
	if _, err := os.Stat(layerInfoPath); err == nil {
		file, err := ioutil.ReadFile(layerInfoPath)
		if err != nil {
			l.LogE(err).Error("Error in reading layers.json file")
			return err
		}
		json.Unmarshal(file, &layersdata)
	}

	storedlayersdata := []LayerInfo{}
	for _, layer := range manifest.Layers {
		storedlayerdata := []LayerInfo{}
		layerDigest := strings.Split(layer.Digest, ":")[1]
		storedlayerinfopath := filepath.Join(cvmfs.LayerMetadataPath(CVMFSRepo, layerDigest), "layers.json")
		if _, err := os.Stat(storedlayerinfopath); err == nil {
			file, err := ioutil.ReadFile(storedlayerinfopath)
			if err != nil {
				l.LogE(err).Error("Error in reading layers.json file")
				return err
			}
			json.Unmarshal(file, &storedlayerdata)
		}
		storedlayersdata = append(storedlayersdata, storedlayerdata...)
	}

	// digestMap maps from compressed digest to uncompressed digest
	for _, layerinfo := range storedlayersdata {
		digestMap[layerinfo.CompressedDiffDigest] = layerinfo.ID
	}

	parentMap := make(map[string]string)
	sizeMap := make(map[string]int)
	lastLayer := ""
	for _, layer := range manifest.Layers {
		sizeMap[layer.Digest] = layer.Size
		if lastLayer != "" {
			parentMap[layer.Digest] = digestMap[lastLayer]
		}
		lastLayer = layer.Digest
	}

	for i := range storedlayersdata {
		digest := storedlayersdata[i].CompressedDiffDigest
		storedlayersdata[i].Parent = parentMap[digest]
		storedlayersdata[i].CompressedSize = sizeMap[digest]
	}

	layersdata = append(layersdata, storedlayersdata...)
	jsonLayerInfo, err := json.MarshalIndent(layersdata, "", " ")
	if err != nil {
		l.LogE(err).Error("Error in marshaling json data for layers.json")
		return err
	}

	err = cvmfs.WriteDataToCvmfs(CVMFSRepo, cvmfs.TrimCVMFSRepoPrefix(layerInfoPath), jsonLayerInfo)
	if err != nil {
		l.LogE(err).Error("Error in writing layers.json file")
		return err
	}
	return
}

// create images.json file in podmanStore
func (img *Image) PublishImageInfo(CVMFSRepo string, digestMap map[string]string) (err error) {
	manifest, err := img.GetManifest()
	if err != nil {
		l.LogE(err).Warn("Error in getting the image manifest")
		return
	}
	//create images.json file
	l.Log().WithFields(log.Fields{"action": "Ingesting images.json in podman store"}).Info(img.GetSimpleName())
	imagedata := []ImageInfo{}
	imageInfoPath := filepath.Join("/", "cvmfs", CVMFSRepo, rootPath, imageMetadataDir, "images.json")

	//check if images.json already exist and append to data
	if _, err := os.Stat(imageInfoPath); err == nil {
		file, err := ioutil.ReadFile(imageInfoPath)
		if err != nil {
			l.LogE(err).Error("Error in reading images.json file")
			return err
		}
		json.Unmarshal(file, &imagedata)
	}

	topLayerDigest := manifest.Layers[len(manifest.Layers)-1].Digest

	id := strings.Split(manifest.Config.Digest, ":")[1]
	topLayer := digestMap[topLayerDigest]
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
		l.LogE(err).Error("Error in marshaling json data for images.json")
		return err
	}

	err = cvmfs.WriteDataToCvmfs(CVMFSRepo, cvmfs.TrimCVMFSRepoPrefix(imageInfoPath), imgInfo)
	if err != nil {
		l.LogE(err).Error("Error in writing images.json")
		return err
	}
	return nil
}

// Ingest the exploded rootfs in podman store.
func (img *Image) LinkRootfsIntoPodmanStore(CVMFSRepo, subDirInsideRepo string, digestMap map[string]string) (err error) {
	l.Log().WithFields(log.Fields{"action": "Ingesting layer rootfs into podman store for the image"}).Info(img.GetSimpleName())
	manifest, err := img.GetManifest()
	if err != nil {
		l.LogE(err).Warn("Error in getting the image manifest")
		return err
	}
	// this is extremelly bad, we are making one transaction for each layer
	// all of them could be done in a single transaction
	for _, layer := range manifest.Layers {
		layerid := strings.Split(layer.Digest, ":")[1]
		layerdir := digestMap[layer.Digest]

		//symlinkPath will contain the rootfs of the corresponding layer in podman store.
		symlinkPath := filepath.Join(rootPath, rootfsDir, layerdir, "diff")
		targetPath := filepath.Join(subDirInsideRepo, layerid[:2], layerid, "layerfs")

		if _, err := os.Stat(symlinkPath); os.IsNotExist(err) {
			err = cvmfs.CreateSymlinkIntoCVMFS(CVMFSRepo, symlinkPath, targetPath)
			if err != nil {
				l.LogE(err).Error("Error in creating the symlink for the diff dir")
				return err
			}
		}
	}
	return nil
}

// Create the link dir and link file for exploded rootfs.
func (img *Image) CreateLinkDir(CVMFSRepo, subDirInsideRepo string, digestMap, layerIdMap map[string]string) (err error) {
	l.Log().WithFields(log.Fields{"action": "Creating Link files for layer rootfs for the image"}).Info(img.GetSimpleName())
	manifest, err := img.GetManifest()
	if err != nil {
		l.LogE(err).Warn("Error in getting the image manifest")
		return err
	}
	// also here, we are making one transaction for each layer
	for _, layer := range manifest.Layers {
		layerdir := digestMap[layer.Digest]
		linkPath := filepath.Join(rootPath, rootfsDir, layerdir, "link")
		if _, err := os.Stat(linkPath); os.IsNotExist(err) {
			//generate the link id
			lid, err := generateID(26)
			if err != nil {
				l.LogE(err).Error("Error generating file name for Link dir")
				return err
			}
			layerIdMap[layer.Digest] = filepath.Join("l", lid)
			err = cvmfs.WriteDataToCvmfs(CVMFSRepo, linkPath, []byte(lid))
			if err != nil {
				l.LogE(err).Error("Error in writing link id to podman store")
				return err
			}

			//Create link dir
			symlinkPath := filepath.Join(rootPath, rootfsDir, "l", lid)
			targetPath := filepath.Join(rootPath, rootfsDir, layerdir, "diff")

			err = cvmfs.CreateSymlinkIntoCVMFS(CVMFSRepo, symlinkPath, targetPath)
			if err != nil {
				l.LogE(err).Error("Error in creating the symlink for the Link dir")
				return err
			}
		} else {
			data, err := ioutil.ReadFile(linkPath)
			if err != nil {
				l.LogE(err).Error("Error in reading link file")
				return err
			}
			layerIdMap[layer.Digest] = string(data)
		}
	}
	return nil
}

// Create the lower files for diff dirs to be used by podman.
func (img *Image) CreateLowerFiles(CVMFSRepo string, digestMap, layerIdMap map[string]string) (err error) {
	l.Log().WithFields(log.Fields{"action": "Creating Lower files for diff dirs in podman store"}).Info(img.GetSimpleName())
	manifest, err := img.GetManifest()
	if err != nil {
		l.LogE(err).Warn("Error in getting the image manifest")
		return err
	}

	lastLowerData := ""
	lastDigest := ""
	// again, for each layer, one more transaction
	for _, layer := range manifest.Layers {
		if lastDigest != "" {
			layerdir := digestMap[layer.Digest]
			lowerPath := filepath.Join(rootPath, rootfsDir, layerdir, "lower")
			if _, err := os.Stat(lowerPath); os.IsNotExist(err) {
				lowerdata := layerIdMap[lastDigest]
				if lastLowerData != "" {
					lowerdata = lowerdata + ":" + lastLowerData
				}
				err = cvmfs.WriteDataToCvmfs(CVMFSRepo, lowerPath, []byte(lowerdata))
				if err != nil {
					l.LogE(err).Warn("Error in writing lower files")
					return err
				}
				lastLowerData = lowerdata
			} else {
				data, err := ioutil.ReadFile(lowerPath)
				if err != nil {
					l.LogE(err).Error("Error in reading lower file")
					return err
				}
				lastLowerData = string(data)
			}
		}
		lastDigest = layer.Digest
	}
	return nil
}

// Ingest the image config file in podman store.
func (img *Image) CreateConfigFile(CVMFSRepo string) (err error) {
	l.Log().WithFields(log.Fields{"action": "Creating config file for the image in podman store"}).Info(img.GetSimpleName())
	manifest, err := img.GetManifest()
	if err != nil {
		l.LogE(err).Warn("Error in getting the image manifest")
		return err
	}
	//generate config file path.
	fname, err := generateConfigFileName(manifest.Config.Digest)
	if err != nil {
		l.LogE(err).Warning("Error in generating config file name")
		return err
	}

	imageID := strings.Split(manifest.Config.Digest, ":")[1]
	configFilePath := filepath.Join(rootPath, imageMetadataDir, imageID, fname)
	if _, err := os.Stat(configFilePath); os.IsNotExist(err) {
		user := img.User
		pass, err := GetPassword()
		if err != nil {
			l.LogE(err).Warning("Unable to get the credential for downloading the configuration blob, trying anonymously")
			user = ""
			pass = ""
		}

		configUrl := fmt.Sprintf("%s://%s/v2/%s/blobs/%s",
			img.Scheme, img.Registry, img.Repository, manifest.Config.Digest)

		token, err := firstRequestForAuth(configUrl, user, pass)
		if err != nil {
			l.LogE(err).Warning("Unable to retrieve the token for downloading config file")
			return err
		}

		client := &http.Client{}
		req, err := http.NewRequest("GET", configUrl, nil)
		if err != nil {
			l.LogE(err).Warning("Unable to create a request for getting config file.")
			return err
		}
		req.Header.Set("Authorization", token)
		req.Header.Set("Accept", "application/vnd.docker.distribution.manifest.v2+json")

		resp, err := client.Do(req)
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			l.LogE(err).Warning("Error in reading the body from the configuration")
			return err
		}

		err = cvmfs.WriteDataToCvmfs(CVMFSRepo, configFilePath, []byte(body))
		if err != nil {
			l.LogE(err).Warning("Error in writing config file")
			return err
		}
	}
	return nil
}

// Ingest the image manifest in podman store.
func (img *Image) PublishImageManifest(CVMFSRepo string) (err error) {
	l.Log().WithFields(log.Fields{"action": "Creating manifest file for the image in podman store"}).Info(img.GetSimpleName())
	manifest, err := img.GetManifest()
	if err != nil {
		l.LogE(err).Warn("Error in getting the image manifest")
		return err
	}
	imageID := strings.Split(manifest.Config.Digest, ":")[1]

	symlinkPath := filepath.Join(rootPath, imageMetadataDir, imageID, "manifest")
	targetPath := filepath.Join(".metadata", img.Registry, img.Repository+img.GetReference(), "manifest.json")

	err = cvmfs.CreateSymlinkIntoCVMFS(CVMFSRepo, symlinkPath, targetPath)
	if err != nil {
		l.LogE(err).Error("Error in creating the symlink for manifest.json")
		return err
	}
	return nil
}

// Create images.lock and layers.lock file to be used by podman.
// Libpod expects these files to be present in its image stores.
func (img *Image) CreateLockFiles(CVMFSRepo string) (err error) {
	l.Log().WithFields(log.Fields{"action": "Creating lock file for the image"}).Info(img.GetSimpleName())
	layerlockpath := filepath.Join("/cvmfs", CVMFSRepo, rootPath, layerMetadataDir, "layers.lock")
	imagelockpath := filepath.Join("/cvmfs", CVMFSRepo, rootPath, imageMetadataDir, "images.lock")
	var paths []string
	if _, err := os.Stat(layerlockpath); os.IsNotExist(err) {
		paths = append(paths, layerlockpath)
	}
	if _, err := os.Stat(imagelockpath); os.IsNotExist(err) {
		paths = append(paths, imagelockpath)
	}
	// two more transaction that could be one
	for _, file := range paths {
		if _, err := os.Stat(file); os.IsNotExist(err) {
			tmpFile, err := ioutil.TempFile("", "lock")
			tmpFile.Close()
			if err != nil {
				return err
			}
			err = cvmfs.PublishToCVMFS(CVMFSRepo, cvmfs.TrimCVMFSRepoPrefix(file), tmpFile.Name())
			os.RemoveAll(tmpFile.Name())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// checks if older version of the image with same tag exists or not in the store.
// If an older version is found, removes it from store and updates images.json file.
// Note: The layers are not removed. Only the manifest, config file and images.json are updated.
func (img *Image) CheckImageChanged(CVMFSRepo string) error {
	l.Log().WithFields(log.Fields{"action": "checking if old image version with same tag exists"}).Info(img.GetSimpleName())
	path := filepath.Join("/cvmfs", CVMFSRepo, rootPath, imageMetadataDir, "images.json")
	if _, err := os.Stat(path); err == nil {
		var imagesinfo []ImageInfo
		newimagesinfo := []ImageInfo{}

		file, err := ioutil.ReadFile(path)
		if err != nil {
			l.LogE(err).Error("error in reading images.json file")
			return err
		}

		json.Unmarshal(file, &imagesinfo)
		for _, image := range imagesinfo {
			present := false
			for _, name := range image.Names {
				// we have already checked if the same image is present in the podman store or not.
				// hence if the we find another image with same name, it means its content (digest) has changed.
				if name == img.GetSimpleName() {
					l.Log().WithFields(log.Fields{"image": img.GetSimpleName()}).Info("older image version present, cleaning and ingesting newer version")
					present = true
					err := cvmfs.RemoveDirectory(CVMFSRepo, rootPath, imageMetadataDir, image.ID)
					if err != nil {
						l.LogE(err).Error("error while removing older image version from podman store")
						return err
					}
					break
				}
			}
			if !present {
				newimagesinfo = append(newimagesinfo, image)
			}
		}
		imgInfo, err := json.MarshalIndent(newimagesinfo, "", " ")
		if err != nil {
			l.LogE(err).Error("Error in marshaling json data for images.json")
			return err
		}

		err = cvmfs.WriteDataToCvmfs(CVMFSRepo, cvmfs.TrimCVMFSRepoPrefix(path), imgInfo)
		if err != nil {
			l.LogE(err).Error("Error in writing images.json")
			return err
		}
	}
	return nil
}

// Ingest all the necessary files and dir in podmanStore dir.
func (img *Image) CreatePodmanImageStore(CVMFSRepo, subDirInsideRepo string) (err error) {
	// this code is extremelly slow
	// if opens and publish several transaction and I believe it is possible to do pretty much anything in a single transaction
	// it should be rewritten.
	n := notification.NewNotification(NotificationService).AddField("image", img.GetSimpleName())
	n.Action("start_podman_ingestion").Send()
	t := time.Now()
	defer func() {
		n.Elapsed(t).Action("end_podman_ingestion").Send()
	}()

	err = img.CheckImageChanged(CVMFSRepo)
	if err != nil {
		l.LogE(err).Error("error while checking if older image version with same tag present in store")
		return err
	}

	l.Log().WithFields(log.Fields{"action": "Ingest the image into podman store"}).Info(img.GetSimpleName())
	createCatalogIntoDirs := []string{rootPath, filepath.Join(rootPath, rootfsDir), filepath.Join(rootPath, imageMetadataDir), filepath.Join(rootPath, layerMetadataDir)}
	for _, dir := range createCatalogIntoDirs {
		err = cvmfs.CreateCatalogIntoDir(CVMFSRepo, dir)
		if err != nil {
			l.LogE(err).WithFields(log.Fields{
				"directory": dir}).Error(
				"Impossible to create subcatalog in the directory.")
		}
	}

	err = img.CreateConfigFile(CVMFSRepo)
	if err != nil {
		l.LogE(err).Error("Unable to create config file in podman store")
		return err
	}

	// digestMap[CompressedDigest] -> UncompressedDigest
	// layerIdMap[CompressedDigest] -> RandomIdOfLayer
	digestMap := make(map[string]string)
	layerIdMap := make(map[string]string)

	// no one read the layers.json file
	err = img.PublishLayerInfo(CVMFSRepo, digestMap)
	if err != nil {
		l.LogE(err).Error("Unable to create layers.json file in podman store")
		return err
	}

	err = img.PublishImageInfo(CVMFSRepo, digestMap)
	if err != nil {
		l.LogE(err).Error("Unable to create images.json file in podman store")
		return err
	}

	err = img.LinkRootfsIntoPodmanStore(CVMFSRepo, subDirInsideRepo, digestMap)
	if err != nil {
		l.LogE(err).Error("Error ingesting rootfs into podman image store")
		return err
	}

	err = img.CreateLinkDir(CVMFSRepo, subDirInsideRepo, digestMap, layerIdMap)
	if err != nil {
		l.LogE(err).Error("Unable to create the link dir in podman store")
		return err
	}

	err = img.CreateLowerFiles(CVMFSRepo, digestMap, layerIdMap)
	if err != nil {
		l.LogE(err).Error("Unable to create the lower files in podman store")
		return err
	}

	err = img.CreateLockFiles(CVMFSRepo)
	if err != nil {
		l.LogE(err).Error("Unable to create lock files in podman store")
		return err
	}

	err = img.PublishImageManifest(CVMFSRepo)
	if err != nil {
		l.LogE(err).Error("Unable to create manifest file in podman store")
		return err
	}
	l.Log().Info("Image successfully ingested into podmanStore")
	return nil
}
