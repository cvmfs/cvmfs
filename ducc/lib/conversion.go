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
	"strconv"
	"strings"
	"sync"
	"time"

	da "github.com/cvmfs/ducc/docker-api"

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

var subDirInsideRepo = ".layers"

func assureValidSingularity() error {
	err, stdout, _ := ExecCommand("singularity", "version").StartWithOutput()
	if err != nil {
		err := fmt.Errorf("No working version of Singularity: %s", err)
		LogE(err).Error("No working version of Singularity")
		return err
	}
	version := stdout.String()
	sems := strings.Split(version, ".")
	if len(sems) < 2 {
		err := fmt.Errorf("Singularity version returned an unexpected format, unable to find Major and Minor number")
		LogE(err).WithFields(log.Fields{"version": version}).Error("Not valid singularity")
		return err
	}
	majorS := sems[0]
	majorI, err := strconv.Atoi(majorS)
	if err != nil {
		errF := fmt.Errorf("Singularity version returned an unexpected format, unable to parse Major number: %s", err)
		LogE(errF).WithFields(log.Fields{"version": version, "major number": majorS}).Error("Not valid singularity")
		return errF
	}
	if majorI >= 4 {
		return nil
	}
	minorS := sems[1]
	minorI, err := strconv.Atoi(minorS)
	if err != nil {
		errF := fmt.Errorf("Singularity version returned an unexpected format, unable to parse Minor number: %s", err)
		LogE(errF).WithFields(log.Fields{"version": version, "minor number": minorS}).Error("Not valid singularity")
		return errF
	}
	if majorI >= 3 && minorI >= 5 {
		return nil
	}
	errF := fmt.Errorf("Installed singularity is too old, we need at least 3.5: Installed version: %s", version)
	LogE(errF).WithFields(log.Fields{"version": version}).Error("Too old singularity")
	return errF
}

func ConvertWishSingularity(wish WishFriendly) (err error) {
	err = assureValidSingularity()
	if err != nil {
		return err
	}
	err = CreateCatalogIntoDir(wish.CvmfsRepo, ".flat")
	if err != nil {
		LogE(err).WithFields(log.Fields{
			"directory": ".flat"}).Error(
			"Impossible to create subcatalog in the directory.")
	}

	tmpDir, err := UserDefinedTempDir("", "conversion")
	if err != nil {
		LogE(err).Error("Error when creating tmp singularity directory")
		return
	}
	defer os.RemoveAll(tmpDir)
	var firstError error
	for _, inputImage := range wish.ExpandedTagImagesFlat {
		// we want to check if we have already ingested the Singularity image
		// Several cases are possible
		// Image not ingested, neither pubSymPath nor privatePath are present
		// Image ingested and up to date, pubSymPath and privatePath point to the same thing
		// Image update but stale (old), pubSymPath and privatePath point to different things
		publicSymlinkPath := inputImage.GetPublicSymlinkPath()
		completePubSymPath := filepath.Join("/", "cvmfs", wish.CvmfsRepo, publicSymlinkPath)
		pubDirInfo, errPub := os.Stat(completePubSymPath)

		singularityPrivatePath, err := inputImage.GetSingularityPath()
		if err != nil {
			errF := fmt.Errorf("Error in getting the path where to save Singularity filesystem: %s", err)
			LogE(err).Warning(errF)
			firstError = errF
			continue
		}
		completeSingularityPriPath := filepath.Join("/", "cvmfs", wish.CvmfsRepo, singularityPrivatePath)
		priDirInfo, errPri := os.Stat(completeSingularityPriPath)

		Log().WithFields(log.Fields{
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
				Log().WithFields(log.Fields{"image": inputImage.GetSimpleName()}).Info("Singularity Image up to date")
				continue
			}
			// delete the old pubLink
			// make a new Link to the privatePaht
			// after that skip and continue
			Log().WithFields(log.Fields{"image": inputImage.GetSimpleName()}).Info("Updating Singularity Image")
			err = CreateSymlinkIntoCVMFS(wish.CvmfsRepo, publicSymlinkPath, singularityPrivatePath)
			if err != nil {
				errF := fmt.Errorf("Error in updating symlink for singularity image: %s", inputImage.GetSimpleName())
				LogE(errF).WithFields(
					log.Fields{"to": publicSymlinkPath, "from": singularityPrivatePath}).
					Error("Error in creating symlink")
				if firstError == nil {
					firstError = errF
				}
			}
			continue
		}

		// no error in stating the private directory, but the public one does not exists
		// we simply create the public directory
		if errPri == nil && os.IsNotExist(errPub) {
			Log().WithFields(log.Fields{"image": inputImage.GetSimpleName()}).Info("Creating link for Singularity Image")
			err = CreateSymlinkIntoCVMFS(wish.CvmfsRepo, publicSymlinkPath, singularityPrivatePath)
			if err != nil {
				errF := fmt.Errorf("Error in creating symlink for singularity image: %s", inputImage.GetSimpleName())
				LogE(errF).WithFields(
					log.Fields{"to": publicSymlinkPath, "from": singularityPrivatePath}).
					Error("Error in creating symlink")
				if firstError == nil {
					firstError = errF
				}
			}
			continue
		}

		singularity, err := inputImage.DownloadSingularityDirectory(tmpDir)
		if err != nil {
			LogE(err).Error("Error in dowloading the singularity image")
			firstError = err
			os.RemoveAll(singularity.TempDirectory)
			continue
		}

		err = singularity.PublishToCVMFS(wish.CvmfsRepo)
		if err != nil {
			LogE(err).Error("Error in ingesting the singularity image into the CVMFS repository")
			firstError = err
			os.RemoveAll(singularity.TempDirectory)
			continue
		}
		os.RemoveAll(singularity.TempDirectory)
	}

	return firstError
}

func ConvertWishDocker(wish WishFriendly) (err error) {
	inputImage := wish.InputImage
	if inputImage == nil {
		err = fmt.Errorf("error in parsing the input image, got a null image")
		LogE(err).WithFields(log.Fields{"input image": wish.InputName}).
			Error("Null image, should not happen")
		return
	}
	outputImage := wish.OutputImage
	if outputImage == nil {
		err = fmt.Errorf("error in parsing the output image, got a null image")
		LogE(err).WithFields(log.Fields{"output image": wish.OutputName}).
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
			Log().Info("Layers not downloaded yet, not converting for docker, moving on")
			continue
		}
		manifest, err := expandedImgTag.GetManifest()
		if err != nil {
			return err
		}
		layerLocations := make(map[string]string)
		for _, layer := range manifest.Layers {
			layerDigest := strings.Split(layer.Digest, ":")[1]
			layerPath := LayerRootfsPath(wish.CvmfsRepo, layerDigest)
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
				Log().Info("Image already present in podman store, moving on")
				continue
			}
		}

		// convert for podman only after manifest is stored in .metadata
		manifestPath = filepath.Join("/", "cvmfs", wish.CvmfsRepo, ".metadata", expandedImgTag.GetSimpleName(), "manifest.json")
		if _, err := os.Stat(manifestPath); os.IsNotExist(err) {
			Log().Info("Layers not downloaded yet, not converting for podman, moving on")
			continue
		}

		err = expandedImgTag.CreatePodmanImageStore(wish.CvmfsRepo, subDirInsideRepo)
		if err != nil && firstError == nil {
			firstError = err
		}
	}
	return firstError
}

func ConvertWish(wish WishFriendly, convertAgain, forceDownload bool) (err error) {
	err = CreateCatalogIntoDir(wish.CvmfsRepo, subDirInsideRepo)
	if err != nil {
		LogE(err).WithFields(log.Fields{
			"directory": subDirInsideRepo}).Error(
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
	Log().WithFields(log.Fields{"alreadyConverted": alreadyConverted}).Info(
		"Already converted the image, skipping.")

	if alreadyConverted == ConversionMatch {
		if convertAgain == false {
			return nil
		}
	}

	layersChanell := make(chan downloadedLayer, 3)
	manifestChanell := make(chan string, 1)
	stopGettingLayers := make(chan bool, 1)
	noErrorInConversion := make(chan bool, 1)

	layerDigestChan := make(chan string, 3)
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
		cleanup := func(location string) {
			Log().Info("Running clean up function deleting the last layer.")

			err := ExecCommand("cvmfs_server", "abort", "-f", repo).Start()
			if err != nil {
				LogE(err).Warning("Error in the abort command inside the cleanup function, this warning is usually normal")
			}

			err = ExecCommand("cvmfs_server", "ingest", "--delete", location, repo).Start()
			if err != nil {
				LogE(err).Error("Error in the cleanup command")
			}
		}
		for layer := range layersChanell {

			Log().WithFields(log.Fields{"layer": layer.Name}).Info("Start Ingesting the file into CVMFS")
			layerDigest := strings.Split(layer.Name, ":")[1]
			layerPath := LayerRootfsPath(repo, layerDigest)

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

				// need to create the "super-directory", those
				// directory starting with 2 char prefix of the
				// digest itself, and put a .cvmfscatalog files
				// in it, if the directory still doesn't
				// exists. Similarly we need a .cvmfscatalog in
				// the layerfs directory, the one that host the
				// whole layer

				for _, dir := range []string{
					filepath.Dir(filepath.Dir(TrimCVMFSRepoPrefix(layerPath))),
					//TrimCVMFSRepoPrefix(layerPath)} {
				} {

					Log().WithFields(log.Fields{"catalogdirectory": dir}).Info("Working on CATALOGDIRECTORY")
					err = CreateCatalogIntoDir(repo, dir)
					if err != nil {
						LogE(err).WithFields(log.Fields{
							"directory": dir}).Error(
							"Impossible to create subcatalog in super-directory.")
					} else {
						Log().WithFields(log.Fields{
							"directory": dir}).Info(
							"Created subcatalog in directory")
					}
				}
				err = ExecCommand("cvmfs_server", "ingest", "--catalog", "-t", "-", "-b", TrimCVMFSRepoPrefix(layerPath), repo).StdIn(layer.Path).Start()

				if err != nil {
					LogE(err).WithFields(log.Fields{"layer": layer.Name}).Error("Some error in ingest the layer")
					noErrors = false
					cleanup(TrimCVMFSRepoPrefix(layerPath))
					return
				}

				err = StoreLayerInfo(repo, layerDigest, layer.Path)
				if err != nil {
					LogE(err).Error("Error in storing the layers.json file in the repository")
				}
				Log().WithFields(log.Fields{"layer": layer.Name}).Info("Finish Ingesting the file")
			} else {
				Log().WithFields(log.Fields{"layer": layer.Name}).Info("Skipping ingestion of layer, already exists")
			}
			//os.Remove(layer.Path)
		}
		Log().Info("Finished pushing the layers into CVMFS")
	}()
	// we create a temp directory for all the files needed, when this function finish we can remove the temp directory cleaning up
	tmpDir, err := UserDefinedTempDir("", "conversion")
	if err != nil {
		LogE(err).Error("Error in creating a temporary direcotry for all the files")
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

	err = SaveLayersBacklink(repo, inputImage, layerDigests)
	if err != nil {
		LogE(err).Error("Error in saving the backlinks")
		noErrorInConversionValue = false
	}

	if noErrorInConversionValue {
		manifestPath := filepath.Join(".metadata", inputImage.GetSimpleName(), "manifest.json")
		errIng := PublishToCVMFS(repo, manifestPath, <-manifestChanell)
		if errIng != nil {
			LogE(errIng).Error("Error in storing the manifest in the repository")
		}
		var errRemoveSchedule error
		if alreadyConverted == ConversionNotMatch {
			Log().Info("Image already converted, but it does not match the manifest, adding it to the remove scheduler")
			errRemoveSchedule = AddManifestToRemoveScheduler(repo, manifest)
			if errRemoveSchedule != nil {
				Log().Warning("Error in adding the image to the remove schedule")
				return errRemoveSchedule
			}
		}
		if errIng == nil && errRemoveSchedule == nil {
			Log().Info("Conversion completed")
			return nil
		}
		return
	} else {
		Log().Warn("Some error during the conversion, we are not storing it into the database")
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
		LogE(err).Error("Error in image import")
		return
	}
	defer importResult.Close()
	Log().Info("Created the image in the local docker daemon")

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
	Log().WithFields(log.Fields{"action": "prepared thin-image manifest"}).Info(string(b))
	defer res.Close()
	// here is possible to use the result of the above ReadAll to have
	// informantion about the status of the upload.
	_, errReadDocker := ioutil.ReadAll(res)
	if err != nil {
		LogE(errReadDocker).Warning("Error in reading the status from docker")
	}
	Log().Info("Finish pushing the image to the registry")
	return
}

func StoreLayerInfo(CVMFSRepo string, layerDigest string, r *ReadAndHash) (err error) {
	Log().WithFields(log.Fields{"action": "Ingesting layers.json"}).Info("store layer information in .layers")
	layersdata := []LayerInfo{}
	layerInfoPath := filepath.Join(LayerMetadataPath(CVMFSRepo, layerDigest), "layers.json")

	diffID := fmt.Sprintf("%x", r.Sum256(nil))
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
		LogE(err).Error("Error in marshaling json data for layers.json")
		return err
	}

	err = writeDataToCvmfs(CVMFSRepo, TrimCVMFSRepoPrefix(layerInfoPath), jsonLayerInfo)
	if err != nil {
		LogE(err).Error("Error in writing layers.json file")
		return err
	}
	return
}

func AlreadyConverted(manifestPath, reference string) ConversionResult {

	fmt.Println(manifestPath)
	manifestStat, err := os.Stat(manifestPath)
	if os.IsNotExist(err) {
		Log().Info("Manifest not existing")
		return ConversionNotFound
	}
	if !manifestStat.Mode().IsRegular() {
		Log().Info("Manifest not a regular file")
		return ConversionNotFound
	}

	manifestFile, err := os.Open(manifestPath)
	if err != nil {
		Log().Info("Error in opening the manifest")
		return ConversionNotFound
	}
	defer manifestFile.Close()

	bytes, _ := ioutil.ReadAll(manifestFile)

	var manifest da.Manifest
	err = json.Unmarshal(bytes, &manifest)
	if err != nil {
		LogE(err).Warning("Error in unmarshaling the manifest")
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
