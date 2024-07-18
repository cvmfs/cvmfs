package util

var NoPasswordError = 101

// Docker conversions remain to adopt to new structure!

/* func ConvertWishDocker(wish WishFriendly) (err error) {
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

func GetPassword() (string, error) {
	envVar := "DUCC_OUTPUT_REGISTRY_PASS"
	pass := os.Getenv(envVar)
	if pass == "" {
		err := fmt.Errorf(
			"Env variable (%s) storing the password to push thin images is not set", envVar)
		return "", err
	}
	return pass, nil
}
*/
