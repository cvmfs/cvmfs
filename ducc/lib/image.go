package lib

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"

	"github.com/docker/docker/image"
	"github.com/olekukonko/tablewriter"
	log "github.com/sirupsen/logrus"

	da "github.com/cvmfs/ducc/docker-api"
)

type ManifestRequest struct {
	Image    Image
	Password string
}

type Image struct {
	Id         int
	User       string
	Scheme     string
	Registry   string
	Repository string
	Tag        string
	Digest     string
	IsThin     bool
	Manifest   *da.Manifest
}

func (i Image) GetSimpleName() string {
	name := fmt.Sprintf("%s/%s", i.Registry, i.Repository)
	if i.Tag == "" {
		return name
	} else {
		return name + ":" + i.Tag
	}
}

func (i Image) WholeName() string {
	root := fmt.Sprintf("%s://%s/%s", i.Scheme, i.Registry, i.Repository)
	if i.Tag != "" {
		root = fmt.Sprintf("%s:%s", root, i.Tag)
	}
	if i.Digest != "" {
		root = fmt.Sprintf("%s@%s", root, i.Digest)
	}
	return root
}

func (i Image) GetManifestUrl() string {
	url := fmt.Sprintf("%s://%s/v2/%s/manifests/", i.Scheme, i.Registry, i.Repository)
	if i.Digest != "" {
		url = fmt.Sprintf("%s%s", url, i.Digest)
	} else {
		url = fmt.Sprintf("%s%s", url, i.Tag)
	}
	return url
}

func (i Image) GetReference() string {
	if i.Digest == "" && i.Tag != "" {
		return ":" + i.Tag
	}
	if i.Digest != "" && i.Tag == "" {
		return "@" + i.Digest
	}
	if i.Digest != "" && i.Tag != "" {
		return ":" + i.Tag + "@" + i.Digest
	}
	panic("Image wrong format, missing both tag and digest")
}

func (i Image) GetSimpleReference() string {
	if i.Tag != "" {
		return i.Tag
	}
	if i.Digest != "" {
		return i.Digest
	}
	panic("Image wrong format, missing both tag and digest")
}

func (img Image) PrintImage(machineFriendly, csv_header bool) {
	if machineFriendly {
		if csv_header {
			fmt.Printf("name,user,scheme,registry,repository,tag,digest,is_thin\n")
		}
		fmt.Printf("%s,%s,%s,%s,%s,%s,%s,%s\n",
			img.WholeName(), img.User, img.Scheme,
			img.Registry, img.Repository,
			img.Tag, img.Digest,
			fmt.Sprint(img.IsThin))
	} else {
		table := tablewriter.NewWriter(os.Stdout)
		table.SetAlignment(tablewriter.ALIGN_LEFT)
		table.SetHeader([]string{"Key", "Value"})
		table.Append([]string{"Name", img.WholeName()})
		table.Append([]string{"User", img.User})
		table.Append([]string{"Scheme", img.Scheme})
		table.Append([]string{"Registry", img.Registry})
		table.Append([]string{"Repository", img.Repository})
		table.Append([]string{"Tag", img.Tag})
		table.Append([]string{"Digest", img.Digest})
		var is_thin string
		if img.IsThin {
			is_thin = "true"
		} else {
			is_thin = "false"
		}
		table.Append([]string{"IsThin", is_thin})
		table.Render()
	}
}

func (img Image) GetManifest() (da.Manifest, error) {
	if img.Manifest != nil {
		return *img.Manifest, nil
	}
	bytes, err := img.getByteManifest()
	if err != nil {
		return da.Manifest{}, err
	}
	var manifest da.Manifest
	err = json.Unmarshal(bytes, &manifest)
	if err != nil {
		return manifest, err
	}
	if reflect.DeepEqual(da.Manifest{}, manifest) {
		return manifest, fmt.Errorf("Got empty manifest")
	}
	img.Manifest = &manifest
	return manifest, nil
}

func (img Image) GetChanges() (changes []string, err error) {
	user := img.User
	pass, err := GetPassword()
	if err != nil {
		LogE(err).Warning("Unable to get the credential for downloading the configuration blog, trying anonymously")
		user = ""
		pass = ""
	}

	changes = []string{"ENV CVMFS_IMAGE true"}
	manifest, err := img.GetManifest()
	if err != nil {
		LogE(err).Warning("Impossible to retrieve the manifest of the image, not changes set")
		return
	}
	configUrl := fmt.Sprintf("%s://%s/v2/%s/blobs/%s",
		img.Scheme, img.Registry, img.Repository, manifest.Config.Digest)
	token, err := firstRequestForAuth(configUrl, user, pass)
	if err != nil {
		LogE(err).Warning("Impossible to retrieve the token for getting the changes from the repository, not changes set")
		return
	}
	client := &http.Client{}
	req, err := http.NewRequest("GET", configUrl, nil)
	if err != nil {
		LogE(err).Warning("Impossible to create a request for getting the changes no chnages set.")
		return
	}
	req.Header.Set("Authorization", token)
	req.Header.Set("Accept", "application/vnd.docker.distribution.manifest.v2+json")

	resp, err := client.Do(req)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		LogE(err).Warning("Error in reading the body from the configuration, no change set")
		return
	}

	var config image.Image
	err = json.Unmarshal(body, &config)
	if err != nil {
		LogE(err).Warning("Error in unmarshaling the configuration of the image")
		return
	}
	env := config.Config.Env

	if len(env) > 0 {
		for _, e := range env {
			envs := strings.SplitN(e, "=", 2)
			if len(envs) != 2 {
				continue
			}
			change := fmt.Sprintf("ENV %s=\"%s\"", envs[0], envs[1])
			changes = append(changes, change)
		}
	}

	cmd := config.Config.Cmd

	if len(cmd) > 0 {
		for _, c := range cmd {
			changes = append(changes, fmt.Sprintf("CMD %s", c))
		}
	}

	return
}

func (img Image) GetSingularityLocation() string {
	return fmt.Sprintf("docker://%s/%s%s", img.Registry, img.Repository, img.GetReference())
}

func GetSingularityPathFromManifest(manifest da.Manifest) string {
	digest := strings.Split(manifest.Config.Digest, ":")[1]
	return filepath.Join(".flat", digest[0:2], digest)
}

// here is where in the FS we are going to store the singularity image
func (img Image) GetSingularityPath() (string, error) {
	manifest, err := img.GetManifest()
	if err != nil {
		LogE(err).Error("Error in getting the manifest to figureout the singularity path")
		return "", err
	}
	return GetSingularityPathFromManifest(manifest), nil
}

type Singularity struct {
	Image         *Image
	TempDirectory string
}

func (img Image) DownloadSingularityDirectory(rootPath string) (sing Singularity, err error) {
	dir, err := ioutil.TempDir(rootPath, "singularity_buffer")
	if err != nil {
		LogE(err).Error("Error in creating temporary directory for singularity")
		return

	}
	singularityTempCache, err := ioutil.TempDir("", "tempDirSingularityCache")
	if err != nil {
		LogE(err).Error("Error in creating temporary directory for singularity cache")
		return
	}
	defer os.RemoveAll(singularityTempCache)
	err = ExecCommand("singularity", "build", "--force", "--sandbox", dir, img.GetSingularityLocation()).Env(
		"SINGULARITY_CACHEDIR", singularityTempCache).Env("PATH", os.Getenv("PATH")).Start()
	if err != nil {
		LogE(err).Error("Error in downloading the singularity image")
		return
	}

	Log().Info("Successfully download the singularity image")
	return Singularity{Image: &img, TempDirectory: dir}, nil
}

func (s Singularity) IngestIntoCVMFS(CVMFSRepo string) error {
	symlinkPath := filepath.Join(s.Image.Registry, s.Image.Repository+":"+s.Image.GetSimpleReference())
	singularityPath, err := s.Image.GetSingularityPath()
	if err != nil {
		LogE(err).Error(
			"Error in ingesting singularity image into CVMFS, unable to get where save the image")
		return err
	}

	err = IngestIntoCVMFS(CVMFSRepo, singularityPath, s.TempDirectory)
	if err != nil {
		// if there is an error ingest does not remove the folder.
		// we do want to remove the folder anyway
		os.RemoveAll(s.TempDirectory)
		return err
	}

	for _, dir := range []string{
		filepath.Dir(singularityPath),
		singularityPath} {

		err = CreateCatalogIntoDir(CVMFSRepo, dir)
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

	// lets create the symlink
	err = CreateSymlinkIntoCVMFS(CVMFSRepo, symlinkPath, singularityPath)
	if err != nil {
		LogE(err).Error("Error in creating the symlink for the singularity Image")
		return err
	}
	return nil
}

func (img Image) getByteManifest() ([]byte, error) {
	pass, err := GetPassword()
	if err != nil {
		LogE(err).Warning("Unable to retrieve the password, trying to get the manifest anonymously.")
		return img.getAnonymousManifest()
	}
	return img.getManifestWithPassword(pass)
}

func (img Image) getAnonymousManifest() ([]byte, error) {
	return getManifestWithUsernameAndPassword(img, "", "")
}

func (img Image) getManifestWithPassword(password string) ([]byte, error) {
	return getManifestWithUsernameAndPassword(img, img.User, password)
}

func getManifestWithUsernameAndPassword(img Image, user, pass string) ([]byte, error) {

	url := img.GetManifestUrl()

	token, err := firstRequestForAuth(url, user, pass)
	if err != nil {
		LogE(err).Error("Error in getting the authentication token")
		return nil, err
	}

	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		LogE(err).Error("Impossible to create a HTTP request")
		return nil, err
	}

	req.Header.Set("Authorization", token)
	req.Header.Set("Accept", "application/vnd.docker.distribution.manifest.v2+json")

	resp, err := client.Do(req)
	if err != nil {
		LogE(err).Error("Error in making the HTTP request")
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		LogE(err).Error("Error in reading the second http response")
		return nil, err
	}
	return body, nil
}

func firstRequestForAuth(url, user, pass string) (token string, err error) {
	resp, err := http.Get(url)
	if err != nil {
		LogE(err).Error("Error in making the first request for auth")
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 401 {
		log.WithFields(log.Fields{
			"status code": resp.StatusCode,
		}).Info("Expected status code 401, print body anyway.")
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			LogE(err).Error("Error in reading the first http response")
		}
		fmt.Println(string(body))
		return "", err
	}
	WwwAuthenticate := resp.Header["Www-Authenticate"][0]
	token, err = requestAuthToken(WwwAuthenticate, user, pass)
	if err != nil {
		LogE(err).Error("Error in getting the authentication token")
		return "", err
	}
	return token, nil

}

func getLayerUrl(img Image, layer da.Layer) string {
	return fmt.Sprintf("%s://%s/v2/%s/blobs/%s",
		img.Scheme, img.Registry, img.Repository, layer.Digest)
}

type downloadedLayer struct {
	Name string
	Path io.ReadCloser
}

func (img Image) GetLayers(layersChan chan<- downloadedLayer, manifestChan chan<- string, stopGettingLayers <-chan bool, rootPath string) error {
	defer close(layersChan)
	defer close(manifestChan)

	user := img.User
	pass, err := GetPassword()
	if err != nil {
		LogE(err).Warning("Unable to retrieve the password, trying to get the layers anonymously.")
		user = ""
		pass = ""
	}

	// then we try to get the manifest from our database
	manifest, err := img.GetManifest()
	if err != nil {
		LogE(err).Warn("Error in getting the manifest")
		return err
	}

	// A first request is used to get the authentication
	firstLayer := manifest.Layers[0]
	layerUrl := getLayerUrl(img, firstLayer)
	token, err := firstRequestForAuth(layerUrl, user, pass)
	if err != nil {
		return err
	}

	killKiller := make(chan bool, 1)
	errorChannel := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {

		select {

		case <-killKiller:
			return
		case <-stopGettingLayers:
			err := fmt.Errorf("Detect errors, stop getting layer")
			errorChannel <- err
			LogE(err).Error("Detect error, stop getting layers")
			cancel()
			return
		}
	}()
	defer func() { killKiller <- true }()

	var wg sync.WaitGroup
	defer wg.Wait()
	// at this point we iterate each layer and we download it.
	for _, layer := range manifest.Layers {
		wg.Add(1)
		go func(ctx context.Context, layer da.Layer) {
			defer wg.Done()
			Log().WithFields(log.Fields{"layer": layer.Digest}).Info("Start working on layer")
			toSend, err := img.downloadLayer(layer, token, rootPath)
			if err != nil {
				LogE(err).Error("Error in downloading a layer")
				return
			}
			select {
			case layersChan <- toSend:
				return
			case <-ctx.Done():
				return
			}
		}(ctx, layer)
	}

	// finally we marshal the manifest and store it into a file
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		LogE(err).Error("Error in marshaling the manifest")
		return err
	}
	manifestPath := filepath.Join(rootPath, "manifest.json")
	err = ioutil.WriteFile(manifestPath, manifestBytes, 0666)
	if err != nil {
		LogE(err).Error("Error in writing the manifest to file")
		return err
	}
	// ship the manifest file
	manifestChan <- manifestPath

	// we wait here to make sure that the channel is populated
	wg.Wait()
	select {
	case err := <-errorChannel:
		return err
	default:
		return nil
	}
}

func (img Image) downloadLayer(layer da.Layer, token, rootPath string) (toSend downloadedLayer, err error) {
	user := img.User
	pass, err := GetPassword()
	if err != nil {
		LogE(err).Warning("Unable to retrieve the password, trying to get the layers anonymously.")
		user = ""
		pass = ""
	}
	layerUrl := getLayerUrl(img, layer)
	if token == "" {
		token, err = firstRequestForAuth(layerUrl, user, pass)
		if err != nil {
			return
		}
	}
	for i := 0; i <= 5; i++ {
		err = nil
		client := &http.Client{}
		req, err := http.NewRequest("GET", layerUrl, nil)
		if err != nil {
			LogE(err).Error("Impossible to create the HTTP request.")
			break
		}
		req.Header.Set("Authorization", token)
		resp, err := client.Do(req)
		Log().WithFields(log.Fields{"layer": layer.Digest}).Info("Make request for layer")
		if err != nil {
			break
		}
		if 200 <= resp.StatusCode && resp.StatusCode < 300 {
			gread, err := gzip.NewReader(resp.Body)
			if err != nil {
				LogE(err).Warning("Error in creating the zip to unzip the layer")
				continue
			}

			toSend = downloadedLayer{Name: layer.Digest, Path: gread}
			return toSend, nil

		} else {
			Log().Warning("Received status code ", resp.StatusCode)
			err = fmt.Errorf("Layer not received, status code: %d", resp.StatusCode)
		}
	}
	return

}

func parseBearerToken(token string) (realm string, options map[string]string, err error) {
	options = make(map[string]string)
	args := token[7:]
	keyValue := strings.Split(args, ",")
	for _, kv := range keyValue {
		splitted := strings.Split(kv, "=")
		if len(splitted) != 2 {
			err = fmt.Errorf("Wrong formatting of the token")
			return
		}
		splitted[1] = strings.Trim(splitted[1], `"`)
		if splitted[0] == "realm" {
			realm = splitted[1]
		} else {
			options[splitted[0]] = splitted[1]
		}
	}
	return
}

func requestAuthToken(token, user, pass string) (authToken string, err error) {
	realm, options, err := parseBearerToken(token)
	if err != nil {
		return
	}
	req, err := http.NewRequest("GET", realm, nil)
	if err != nil {
		return
	}

	query := req.URL.Query()
	for k, v := range options {
		query.Add(k, v)
	}
	if user != "" && pass != "" {
		query.Add("offline_token", "true")
		req.SetBasicAuth(user, pass)
	}
	req.URL.RawQuery = query.Encode()

	client := &http.Client{}
	resp, err := client.Do(req)
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		err = fmt.Errorf("Authorization error %s", resp.Status)
		return
	}

	var jsonResp map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&jsonResp)
	if err != nil {
		return
	}
	authTokenInterface, ok := jsonResp["token"]
	if ok {
		authToken = "Bearer " + authTokenInterface.(string)
	} else {
		err = fmt.Errorf("Didn't get the token key from the server")
		return
	}
	return
}
