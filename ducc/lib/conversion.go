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
	stdout, _, err := ExecCommand("singularity", "version").StartWithOutput()
	if err != nil {
		err := fmt.Errorf("no working version of Singularity: %s", err)
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
	errF := fmt.Errorf("installed singularity is too old, we need at least 3.5: Installed version: %s", version)
	LogE(errF).WithFields(log.Fields{"version": version}).Error("Too old singularity")
	return errF
}

func ConvertWishSingularity(wish WishFriendly) (err error) {
	err = assureValidSingularity()
	if err != nil {
		return err
	}
	tmpDir, err := UserDefinedTempDir("", "conversion")
	if err != nil {
		LogE(err).Error("Error when creating tmp singularity directory")
		return
	}
	defer os.RemoveAll(tmpDir)
	inputImage, err := ParseImage(wish.InputName)
	expandedImgTag, err := inputImage.ExpandWildcard()
	if err != nil {
		LogE(err).WithFields(log.Fields{
			"input image": fmt.Sprintf("%s/%s", inputImage.Registry, inputImage.Repository)}).
			Error("Error in retrieving all the tags from the image")
		return
	}
	var firstError error
	for _, inputImage := range expandedImgTag {
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
			errF := fmt.Errorf("error in getting the path where to save Singularity filesystem: %s", err)
			LogE(err).Warning(errF)
			firstError = errF
			continue
		}
		completeSingularityPriPath := filepath.Join("/", "cvmfs", wish.CvmfsRepo, singularityPrivatePath)
		priDirInfo, errPri := os.Stat(completeSingularityPriPath)

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
				errF := fmt.Errorf("error in updating symlink for singularity image: %s", inputImage.GetSimpleName())
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
				errF := fmt.Errorf("error in creating symlink for singularity image: %s", inputImage.GetSimpleName())
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

		err = singularity.IngestIntoCVMFS(wish.CvmfsRepo)
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

func ConvertWishDocker(wish WishFriendly, convertAgain, forceDownload, createThinImage bool) (err error) {

	err = CreateCatalogIntoDir(wish.CvmfsRepo, subDirInsideRepo)
	if err != nil {
		LogE(err).WithFields(log.Fields{
			"directory": subDirInsideRepo}).Error(
			"Impossible to create subcatalog in super-directory.")
	}
	err = CreateCatalogIntoDir(wish.CvmfsRepo, ".flat")
	if err != nil {
		LogE(err).WithFields(log.Fields{
			"directory": ".flat"}).Error(
			"Impossible to create subcatalog in super-directory.")
	}

	outputImage, err := ParseImage(wish.OutputName)
	outputImage.User = wish.UserOutput
	if err != nil {
		return
	}
	inputImage, err := ParseImage(wish.InputName)
	inputImage.User = wish.UserInput
	if err != nil {
		return
	}
	var firstError error
	expandedImgTags, err := inputImage.ExpandWildcard()
	if err != nil {
		LogE(err).WithFields(log.Fields{
			"input image": fmt.Sprintf("%s/%s", inputImage.Registry, inputImage.Repository)}).
			Error("Error in retrieving all the tags from the image")
		return err
	}
	for _, expandedImgTag := range expandedImgTags {
		tag := expandedImgTag.Tag
		outputWithTag := outputImage
		if inputImage.TagWildcard {
			outputWithTag.Tag = tag
		} else {
			outputWithTag.Tag = outputImage.Tag
		}
		err = convertInputOutput(expandedImgTag, outputWithTag, wish.CvmfsRepo, convertAgain, forceDownload, createThinImage)
		if err != nil && firstError == nil {
			firstError = err
		}
	}
	return firstError
}

func getLayersOfImage(image *Image, repo string, forceDownload bool, layersChan chan<- downloadedLayer) (err error) {
	defer close(layersChan)

	manifest, err := image.GetManifest()
	if err != nil {
		LogE(err).WithFields(log.Fields{"image": image.WholeName(), "error": err}).Error("Error in getting the manifest for the image")
		return err
	}

	// check if layer is needed
	layersToDownload := make([]da.Layer, 0)
	for _, layer := range manifest.Layers {
		layerDigest := strings.Split(layer.Digest, ":")[1]
		layerPath := LayerRootfsPath(repo, layerDigest)
		if _, err := os.Stat(layerPath); os.IsNotExist(err) || forceDownload {
			layersToDownload = append(layersToDownload, layer)
		}
	}
	if len(layersToDownload) == 0 {
		return nil
	}

	user := image.User
	pass, err := GetPassword()
	if err != nil {
		user = ""
		pass = ""
	}
	layerUrl := getBlobUrl(*image, layersToDownload[0].Digest)
	token, err := firstRequestForAuth(layerUrl, user, pass)
	if err != nil {
		LogE(err).WithFields(log.Fields{"error": err}).Error("Error in obtain the authentication token to download the layers.")
		return err
	}
	var wg sync.WaitGroup
	defer wg.Wait()
	for _, layer := range layersToDownload {
		wg.Add(1)
		go func(layer da.Layer) {
			defer wg.Done()
			toSend, err := image.downloadLayer(layer, token)
			if err != nil {
				return
			}
			layersChan <- toSend
		}(layer)
	}

	return nil
}

func convertInputOutput(inputImage, outputImage Image, repo string, convertAgain, forceDownload, createThinImage bool) (err error) {

	manifest, err := inputImage.GetManifest()
	if err != nil {
		return
	}

	alreadyConverted := alreadyConverted(repo, inputImage, manifest.Config.Digest)
	Log().WithFields(log.Fields{"alreadyConverted": alreadyConverted}).Info(
		"Already converted the image, skipping.")

	if alreadyConverted == ConversionMatch {
		if !convertAgain {
			return nil
		}
	}

	layersChanell := make(chan downloadedLayer, 3)
	manifestChanell := make(chan string, 1)
	stopGettingLayers := make(chan bool, 1)
	noErrorInConversion := make(chan bool, 1)

	type LayerRepoLocation struct {
		Digest   string
		Location string //location does NOT need the prefix `/cvmfs`
	}
	layerRepoLocationChan := make(chan LayerRepoLocation, 3)
	layerDigestChan := make(chan string, 3)
	go func() {
		noErrors := true
		var wg sync.WaitGroup
		defer func() {
			wg.Wait()
			close(layerRepoLocationChan)
			close(layerDigestChan)
		}()
		defer func() {
			noErrorInConversion <- noErrors
			stopGettingLayers <- true
			close(stopGettingLayers)
		}()
		cleanup := func(location string) {
			Log().Info("Running clean up function deleting the last layer.")

			err = IngestDelete(repo, location)
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
			go func(layerName, layerLocation, layerDigest string) {
				layerRepoLocationChan <- LayerRepoLocation{
					Digest:   layerName,
					Location: layerLocation}
				layerDigestChan <- layerDigest
				wg.Done()
			}(layer.Name, layerPath, layerDigest)

			if !pathExists || forceDownload {

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
				err = Ingest(repo, TrimCVMFSRepoPrefix(layerPath), true, layer.Path)

				if err != nil {
					LogE(err).WithFields(log.Fields{"layer": layer.Name}).Error("Some error in ingest the layer")
					noErrors = false
					cleanup(TrimCVMFSRepoPrefix(layerPath))
					return
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

	layerLocations := make(map[string]string)
	wg.Add(1)
	go func() {
		for layerLocation := range layerRepoLocationChan {
			layerLocations[layerLocation.Digest] = layerLocation.Location
		}
		wg.Done()
	}()

	var layerDigests []string
	wg.Add(1)
	go func() {
		for layerDigest := range layerDigestChan {
			layerDigests = append(layerDigests, layerDigest)
		}
		wg.Done()
	}()
	wg.Wait()

	if createThinImage {
		err = createDockerThinImage(manifest, layerLocations, inputImage, outputImage)
		if err != nil {
			return
		}
	}

	// we wait for the goroutines to finish
	// and if there was no error we conclude everything writing the manifest into the repository
	noErrorInConversionValue := <-noErrorInConversion

	err = SaveLayersBacklink(repo, inputImage, layerDigests)
	if err != nil {
		LogE(err).Error("Error in saving the backlinks")
		noErrorInConversionValue = false
	}

	if noErrorInConversionValue {
		if createThinImage {
			// is necessary this mechanism to pass the authentication to the
			// dockers even if the documentation says otherwise
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
				err = fmt.Errorf("error in pushing the image: %s", errImgPush)
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
		}
		manifestPath := filepath.Join(".metadata", inputImage.GetSimpleName(), "manifest.json")
		errIng := IngestIntoCVMFS(repo, manifestPath, <-manifestChanell)
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

func createDockerThinImage(manifest da.Manifest, layerLocations map[string]string, inputImage, outputImage Image) (err error) {
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

func alreadyConverted(CVMFSRepo string, img Image, reference string) ConversionResult {
	path := filepath.Join("/", "cvmfs", CVMFSRepo, ".metadata", img.GetSimpleName(), "manifest.json")

	fmt.Println(path)
	manifestStat, err := os.Stat(path)
	if os.IsNotExist(err) {
		Log().Info("Manifest not existing")
		return ConversionNotFound
	}
	if !manifestStat.Mode().IsRegular() {
		Log().Info("Manifest not a regular file")
		return ConversionNotFound
	}

	manifestFile, err := os.Open(path)
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

var alreadyGotPass = false
var passwordFromEnv = ""

func GetPassword() (string, error) {
	envVar := "DUCC_DOCKER_REGISTRY_PASS"
	if alreadyGotPass {
		return passwordFromEnv, nil
	}
	pass := os.Getenv(envVar)
	var err error
	if pass == "" {
		err = fmt.Errorf("Env variable (%s) storing the password to access the docker registry is not set", envVar)
	}
	alreadyGotPass = true
	passwordFromEnv = pass
	return pass, err
}
