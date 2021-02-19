package lib

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	constants "github.com/cvmfs/ducc/constants"
	cvmfs "github.com/cvmfs/ducc/cvmfs"
	da "github.com/cvmfs/ducc/docker-api"
	l "github.com/cvmfs/ducc/log"
	notification "github.com/cvmfs/ducc/notification"
	singularity "github.com/cvmfs/ducc/singularity"
	temp "github.com/cvmfs/ducc/temp"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
	log "github.com/sirupsen/logrus"
)

var NoPasswordError = 101

type ConversionResult int

const (
	ConversionNotFound = iota
	ConversionMatch    = iota
	ConversionNotMatch = iota
)

func ConvertWishFlat(wish WishFriendly) error {
	var firstError = error(nil)

	n := notification.NewNotification(NotificationService).AddField("image_request", wish.InputName)

	nFlat := n.AddField("action", "start_flat_conversion").AddId()
	nFlat.Send()
	tFlat := time.Now()
	defer func() {
		nFlat.Elapsed(tFlat).AddField("action", "end_flat_conversion").Send()
	}()

	// it may happend at the very first round that this two calls return an error, let it be
	if err := cvmfs.CreateCatalogIntoDir(wish.CvmfsRepo, ".chains"); err != nil {
		l.LogE(err).Error("Error in creating catalog inside `.chains` directory")
	}
	if err := cvmfs.CreateCatalogIntoDir(wish.CvmfsRepo, ".flat"); err != nil {
		l.LogE(err).Error("Error in creating catalog inside `.flat` directory")
	}
	for _, inputImage := range wish.ExpandedTagImagesFlat {
		publicSymlinkPath := inputImage.GetPublicSymlinkPath()
		completePubSymPath := filepath.Join("/", "cvmfs", wish.CvmfsRepo, publicSymlinkPath)
		pubDirInfo, errPub := os.Stat(completePubSymPath)

		singularityPrivatePath, err := inputImage.GetSingularityPath()
		if err != nil {
			errF := fmt.Errorf("Error in getting the path where to save Singularity filesystem: %s", err)
			l.LogE(err).Warning(errF)
			firstError = errF
			continue
		}
		completeSingularityPriPath := filepath.Join("/", "cvmfs", wish.CvmfsRepo, singularityPrivatePath)
		priDirInfo, errPri := os.Stat(completeSingularityPriPath)

		l.Log().WithFields(log.Fields{
			"image":                  inputImage.GetSimpleName(),
			"public path":            completePubSymPath,
			"err stats pubblic path": errPub,
			"private path":           completeSingularityPriPath,
			"err stats private path": errPri,
		}).Info("Checking if images links are up to date.")
		// no error in stating both directories
		// either the image is up to date or the image became stale
		if errPub == nil && errPri == nil {
			if os.SameFile(pubDirInfo, priDirInfo) {
				// the link is up to date
				l.Log().WithFields(log.Fields{"image": inputImage.GetSimpleName()}).Info("Singularity Image up to date")
				continue
			}
			// delete the old pubLink
			// make a new Link to the privatePaht
			// after that skip and continue
			l.Log().WithFields(log.Fields{"image": inputImage.GetSimpleName()}).Info("Updating Singularity Image")
			err = cvmfs.CreateSymlinkIntoCVMFS(wish.CvmfsRepo, publicSymlinkPath, singularityPrivatePath)
			if err != nil {
				errF := fmt.Errorf("Error in updating symlink for singularity image: %s", inputImage.GetSimpleName())
				l.LogE(errF).WithFields(
					log.Fields{"to": publicSymlinkPath, "from": singularityPrivatePath}).
					Error("Error in creating symlink")
				if firstError == nil {
					firstError = errF
				}
			}
			if err == nil {
				n.Action("publish_flat_image").AddField("public_path", publicSymlinkPath).AddField("private_path", singularityPrivatePath).Send()
			}
			continue
		}

		// no error in stating the private directory, but the public one does not exists
		// we simply create the public directory
		if errPri == nil && os.IsNotExist(errPub) {
			l.Log().WithFields(log.Fields{"image": inputImage.GetSimpleName()}).Info("Creating link for Singularity Image")
			err = cvmfs.CreateSymlinkIntoCVMFS(wish.CvmfsRepo, publicSymlinkPath, singularityPrivatePath)
			if err != nil {
				errF := fmt.Errorf("Error in creating symlink for singularity image: %s", inputImage.GetSimpleName())
				l.LogE(errF).WithFields(
					log.Fields{"to": publicSymlinkPath, "from": singularityPrivatePath}).
					Error("Error in creating symlink")
				if firstError == nil {
					firstError = errF
				}
			}
			if err == nil {
				n.Action("publish_flat_image").AddField("public_path", publicSymlinkPath).AddField("private_path", singularityPrivatePath).Send()
			}
			continue
		}

		i := n.AddField("image", inputImage.GetSimpleName()).AddId()
		t1 := time.Now()
		i.Action("start_single_chain_conversion").Send()
		i = i.Action("end_single_chain_convertion")

		err, lastChain := inputImage.CreateSneakyChainStructure(wish.CvmfsRepo)
		if err != nil {
			if firstError == nil {
				firstError = err
			}
			l.LogE(err).Error("Error in creating the chain structure")
			i.Error(err).Elapsed(t1).Send()
			continue
		}

		if _, err := os.Stat(filepath.Dir(completeSingularityPriPath)); err != nil {
			cvmfs.WithinTransaction(wish.CvmfsRepo, func() error {
				return os.MkdirAll(filepath.Dir(completeSingularityPriPath), constants.DirPermision)
			})
		}
		ociImage, err := inputImage.GetOCIImage()
		if err != nil {
			if firstError == nil {
				firstError = err
			}
			l.LogE(err).Error("Error in getting the OCI image configuration")
			i.Error(err).Elapsed(t1).Send()
			continue
		}
		// we create the image with the correct singularity's dotfiles
		err = cvmfs.WithinTransaction(wish.CvmfsRepo,
			func() error {
				if err := singularity.MakeBaseEnv(completeSingularityPriPath); err != nil {
					l.LogE(err).Error("Error in creating the base singularity environment")
					return err
				}
				if err := singularity.InsertRunScript(completeSingularityPriPath, ociImage); err != nil {
					l.LogE(err).Error("Error in inserting the singularity runscript")
					return err
				}
				if err := singularity.InsertEnv(completeSingularityPriPath, ociImage); err != nil {
					l.LogE(err).Error("Error in inserting the singularity environment")
					return err
				}
				return nil
			},
			cvmfs.NewTemplateTransaction(
				cvmfs.TrimCVMFSRepoPrefix(cvmfs.ChainPath(wish.CvmfsRepo, lastChain)),
				singularityPrivatePath))

		if err != nil {
			if firstError == nil {
				firstError = err
			}
			l.LogE(err).Error("Error in creating the dotfile inside the flat directory")
			i.Error(err).Elapsed(t1).Send()
			continue
		}
		// we create the public link

		err = cvmfs.CreateSymlinkIntoCVMFS(wish.CvmfsRepo, publicSymlinkPath, singularityPrivatePath)
		if err != nil {
			errF := fmt.Errorf("Error in creating symlink for singularity image: %s", inputImage.GetSimpleName())
			l.LogE(errF).WithFields(
				log.Fields{"to": publicSymlinkPath, "from": singularityPrivatePath}).
				Error("Error in creating symlink")
			if firstError == nil {
				firstError = errF
			}
			i.Error(err).Elapsed(t1).Send()
			continue
		}
		i.Error(err).Elapsed(t1).Send()
		if err == nil {
			n.Action("publish_flat_image").AddField("public_path", publicSymlinkPath).AddField("private_path", singularityPrivatePath).Send()
		}
		continue

	}
	return firstError
}

func ConvertWishDocker(wish WishFriendly) (err error) {
	inputImage := wish.InputImage
	if inputImage == nil {
		err = fmt.Errorf("error in parsing the input image, got a null image")
		l.LogE(err).WithFields(log.Fields{"input image": wish.InputName}).
			Error("Null image, should not happen")
		return
	}
	outputImage := wish.OutputImage
	if outputImage == nil {
		err = fmt.Errorf("error in parsing the output image, got a null image")
		l.LogE(err).WithFields(log.Fields{"output image": wish.OutputName}).
			Error("Null image, should not happen")
		return
	}
	var firstError error
	for _, expandedImgTag := range wish.ExpandedTagImagesLayer {
		tag := expandedImgTag.Tag
		outputWithTag := *outputImage
		if inputImage.TagWildcard {
			outputWithTag.Tag = tag
		} else {
			outputWithTag.Tag = outputImage.Tag
		}

		manifestPath := filepath.Join("/", "cvmfs", wish.CvmfsRepo, ".metadata", expandedImgTag.GetSimpleName(), "manifest.json")
		if _, err := os.Stat(manifestPath); os.IsNotExist(err) {
			l.Log().Info("Layers not downloaded yet, not converting for docker, moving on")
			continue
		}
		manifest, err := expandedImgTag.GetManifest()
		if err != nil {
			return err
		}
		layerLocations := make(map[string]string)
		for _, layer := range manifest.Layers {
			layerDigest := strings.Split(layer.Digest, ":")[1]
			layerPath := cvmfs.LayerRootfsPath(wish.CvmfsRepo, layerDigest)
			layerLocations[layer.Digest] = layerPath
		}
		err = CreateThinImage(manifest, layerLocations, *expandedImgTag, *outputImage)
		if err != nil && firstError == nil {
			firstError = err
		}
		err = PushImageToRegistry(*outputImage)
		if err != nil && firstError == nil {
			firstError = err
		}
	}
	return firstError
}

func ConvertWishPodman(wish WishFriendly, convertAgain bool) (err error) {
	var firstError error
	for _, expandedImgTag := range wish.ExpandedTagImagesLayer {
		manifest, err := expandedImgTag.GetManifest()
		if err != nil {
			return err
		}
		imageID := strings.Split(manifest.Config.Digest, ":")[1]

		// check if image is already present in podman store
		manifestPath := filepath.Join("/", "cvmfs", wish.CvmfsRepo, rootPath, imageMetadataDir, imageID, "manifest")
		alreadyConverted := AlreadyConverted(manifestPath, manifest.Config.Digest)
		if alreadyConverted == ConversionMatch {
			if convertAgain == false {
				l.Log().Info("Image already present in podman store, moving on")
				continue
			}
		}

		// convert for podman only after manifest is stored in .metadata
		manifestPath = filepath.Join("/", "cvmfs", wish.CvmfsRepo, ".metadata", expandedImgTag.GetSimpleName(), "manifest.json")
		if _, err := os.Stat(manifestPath); os.IsNotExist(err) {
			l.Log().Info("Layers not downloaded yet, not converting for podman, moving on")
			continue
		}

		err = expandedImgTag.CreatePodmanImageStore(wish.CvmfsRepo, constants.SubDirInsideRepo)
		if err != nil && firstError == nil {
			firstError = err
		}
	}
	return firstError
}

func ConvertWish(wish WishFriendly, convertAgain, forceDownload bool) (err error) {
	err = cvmfs.CreateCatalogIntoDir(wish.CvmfsRepo, constants.SubDirInsideRepo)
	if err != nil {
		l.LogE(err).WithFields(log.Fields{
			"directory": constants.SubDirInsideRepo}).Error(
			"Impossible to create subcatalog in the directory.")
	}
	var firstError error
	for _, expandedImgTag := range wish.ExpandedTagImagesLayer {
		err = convertInputOutput(expandedImgTag, wish.CvmfsRepo, convertAgain, forceDownload)
		if err != nil && firstError == nil {
			firstError = err
		}
	}
	return firstError
}

func convertInputOutput(inputImage *Image, repo string, convertAgain, forceDownload bool) (err error) {
	manifest, err := inputImage.GetManifest()
	if err != nil {
		return
	}
	manifestPath := filepath.Join("/", "cvmfs", repo, ".metadata", inputImage.GetSimpleName(), "manifest.json")
	alreadyConverted := AlreadyConverted(manifestPath, manifest.Config.Digest)
	l.Log().WithFields(log.Fields{"alreadyConverted": alreadyConverted}).Info(
		"Already converted the image, skipping.")

	if alreadyConverted == ConversionMatch {
		if convertAgain == false {
			return nil
		}
	}

	layersChanell := make(chan downloadedLayer, 10)
	manifestChanell := make(chan string, 1)
	stopGettingLayers := make(chan bool, 1)
	noErrorInConversion := make(chan bool, 1)

	layerDigestChan := make(chan string, 10)

	n := notification.NewNotification(NotificationService)
	n = n.AddField("image", inputImage.GetSimpleName())

	go func() {
		noErrors := true
		var wg sync.WaitGroup
		defer func() {
			wg.Wait()
			close(layerDigestChan)
		}()
		defer func() {
			noErrorInConversion <- noErrors
			stopGettingLayers <- true
			close(stopGettingLayers)
		}()

		for layer := range layersChanell {

			l.Log().WithFields(log.Fields{"layer": layer.Name}).Info("Start Ingesting the file into CVMFS")
			layerDigest := strings.Split(layer.Name, ":")[1]
			layerPath := cvmfs.LayerRootfsPath(repo, layerDigest)

			var pathExists bool
			if _, err := os.Stat(layerPath); os.IsNotExist(err) {
				pathExists = false
			} else {
				pathExists = true
			}

			// need to run this into a goroutine to avoid a deadlock
			wg.Add(1)
			go func(layerDigest string) {
				layerDigestChan <- layerDigest
				wg.Done()
			}(layerDigest)

			if pathExists == false || forceDownload {

				ln := n.AddField("layer", layerDigest).AddId()
				ln.Action("start_layer_conversion").Send()

				t1 := time.Now()
				err = layer.IngestIntoCVMFS(repo)

				ln.Elapsed(t1).Action("end_layer_conversion").Error(err).SizeBytes(layer.GetSize()).Send()

				if err != nil {
					l.LogE(err).Error("Error in ingesting the layer in cvmfs")
					noErrors = false
				}
				if err == nil {
					n.Action("publish_layer").AddField("layer_digest", layerDigest).Send()
				}
				l.Log().WithFields(
					log.Fields{"layer": layer.Name}).
					Info("Finish Ingesting the file")
			} else {
				l.Log().WithFields(
					log.Fields{"layer": layer.Name}).
					Info("Skipping ingestion of layer, already exists")
			}

			layer.Close()
		}
		l.Log().Info("Finished pushing the layers into CVMFS")
	}()
	// we create a temp directory for all the files needed, when this function finish we can remove the temp directory cleaning up
	tmpDir, err := temp.UserDefinedTempDir("", "conversion")
	if err != nil {
		l.LogE(err).Error("Error in creating a temporary directory for all the files")
		return
	}
	defer os.RemoveAll(tmpDir)

	// this wil start to feed the above goroutine by writing into layersChanell
	err = inputImage.GetLayers(layersChanell, manifestChanell, stopGettingLayers, tmpDir)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup

	var layerDigests []string
	wg.Add(1)
	go func() {
		for layerDigest := range layerDigestChan {
			layerDigests = append(layerDigests, layerDigest)
		}
		wg.Done()
	}()
	wg.Wait()

	// we wait for the goroutines to finish
	// and if there was no error we conclude everything writing the manifest into the repository
	noErrorInConversionValue := <-noErrorInConversion

	err = cvmfs.SaveLayersBacklink(repo, manifest, inputImage.GetSimpleName(), layerDigests)
	if err != nil {
		l.LogE(err).Error("Error in saving the backlinks")
		noErrorInConversionValue = false
	}

	if noErrorInConversionValue {
		manifestPath := filepath.Join(".metadata", inputImage.GetSimpleName(), "manifest.json")
		errIng := cvmfs.PublishToCVMFS(repo, manifestPath, <-manifestChanell)
		if errIng != nil {
			l.LogE(errIng).Error("Error in storing the manifest in the repository")
		}
		var errRemoveSchedule error
		if alreadyConverted == ConversionNotMatch {
			l.Log().Info("Image already converted, but it does not match the manifest, adding it to the remove scheduler")
			errRemoveSchedule = cvmfs.AddManifestToRemoveScheduler(repo, manifest)
			if errRemoveSchedule != nil {
				l.Log().Warning("Error in adding the image to the remove schedule")
				return errRemoveSchedule
			}
		}
		if errIng == nil && errRemoveSchedule == nil {
			l.Log().Info("Conversion completed")
			return nil
		}
		return
	} else {
		l.Log().Warn("Some error during the conversion, we are not storing it into the database")
		return
	}
}

func CreateThinImage(manifest da.Manifest, layerLocations map[string]string, inputImage, outputImage Image) (err error) {
	thin, err := da.MakeThinImage(manifest, layerLocations, inputImage.WholeName())
	if err != nil {
		return
	}

	thinJson, err := json.MarshalIndent(thin, "", "  ")
	if err != nil {
		return
	}
	var imageTar bytes.Buffer
	tarFile := tar.NewWriter(&imageTar)
	header := &tar.Header{Name: "thin.json", Mode: 0644, Size: int64(len(thinJson))}
	err = tarFile.WriteHeader(header)
	if err != nil {
		return
	}
	_, err = tarFile.Write(thinJson)
	if err != nil {
		return
	}
	err = tarFile.Close()
	if err != nil {
		return
	}

	dockerClient, err := client.NewClientWithOpts(client.WithVersion("1.19"))
	if err != nil {
		return
	}

	changes, _ := inputImage.GetChanges()
	image := types.ImageImportSource{
		Source:     bytes.NewBuffer(imageTar.Bytes()),
		SourceName: "-",
	}
	importOptions := types.ImageImportOptions{
		Tag:     outputImage.Tag,
		Message: "",
		Changes: changes,
	}
	importResult, err := dockerClient.ImageImport(
		context.Background(),
		image,
		outputImage.GetSimpleName(),
		importOptions)
	if err != nil {
		l.LogE(err).Error("Error in image import")
		return
	}
	defer importResult.Close()
	l.Log().Info("Created the image in the local docker daemon")

	return nil
}

func PushImageToRegistry(outputImage Image) (err error) {
	// the authentication must be provided for the ImagePush api,
	// even if the documentation says otherwise
	password, err := GetPassword()
	if err != nil {
		return err
	}
	authStruct := struct {
		Username string
		Password string
	}{
		Username: outputImage.User,
		Password: password,
	}
	authBytes, _ := json.Marshal(authStruct)
	authCredential := base64.StdEncoding.EncodeToString(authBytes)
	pushOptions := types.ImagePushOptions{
		RegistryAuth: authCredential,
	}
	dockerClient, err := client.NewClientWithOpts(client.WithVersion("1.19"))
	if err != nil {
		return err
	}
	res, errImgPush := dockerClient.ImagePush(
		context.Background(),
		outputImage.GetSimpleName(),
		pushOptions)
	if errImgPush != nil {
		err = fmt.Errorf("Error in pushing the image: %s", errImgPush)
		return err
	}
	b, _ := ioutil.ReadAll(res)
	l.Log().WithFields(log.Fields{"action": "prepared thin-image manifest"}).Info(string(b))
	defer res.Close()
	// here is possible to use the result of the above ReadAll to have
	// informantion about the status of the upload.
	_, errReadDocker := ioutil.ReadAll(res)
	if err != nil {
		l.LogE(errReadDocker).Warning("Error in reading the status from docker")
	}
	l.Log().Info("Finish pushing the image to the registry")
	return
}

func StoreLayerInfo(CVMFSRepo string, layerDigest string, r ReadHashCloseSizer) (err error) {
	l.Log().WithFields(log.Fields{"action": "Ingesting layers.json"}).Info("store layer information in .layers")
	layersdata := []LayerInfo{}
	layerInfoPath := filepath.Join(cvmfs.LayerMetadataPath(CVMFSRepo, layerDigest), "layers.json")

	diffID := fmt.Sprintf("%x", r.Sum(nil))
	size := r.GetSize()
	created := time.Now()
	layerinfo := LayerInfo{
		ID:                   diffID,
		Created:              created,
		CompressedDiffDigest: "sha256:" + layerDigest,
		UncompressedDigest:   "sha256:" + diffID,
		UncompressedSize:     size,
	}
	layersdata = append(layersdata, layerinfo)

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

func AlreadyConverted(manifestPath, reference string) ConversionResult {

	fmt.Println(manifestPath)
	manifestStat, err := os.Stat(manifestPath)
	if os.IsNotExist(err) {
		l.Log().Info("Manifest not existing")
		return ConversionNotFound
	}
	if !manifestStat.Mode().IsRegular() {
		l.Log().Info("Manifest not a regular file")
		return ConversionNotFound
	}

	manifestFile, err := os.Open(manifestPath)
	if err != nil {
		l.Log().Info("Error in opening the manifest")
		return ConversionNotFound
	}
	defer manifestFile.Close()

	bytes, _ := ioutil.ReadAll(manifestFile)

	var manifest da.Manifest
	err = json.Unmarshal(bytes, &manifest)
	if err != nil {
		l.LogE(err).Warning("Error in unmarshaling the manifest")
		return ConversionNotFound
	}
	fmt.Printf("%s == %s\n", manifest.Config.Digest, reference)
	if manifest.Config.Digest == reference {
		return ConversionMatch
	}
	return ConversionNotMatch
}

func GetPassword() (string, error) {
	envVar := "DUCC_DOCKER_REGISTRY_PASS"
	pass := os.Getenv(envVar)
	if pass == "" {
		err := fmt.Errorf(
			"Env variable (%s) storing the password to access the docker registry is not set", envVar)
		return "", err
	}
	return pass, nil
}
