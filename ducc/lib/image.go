package lib

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/image"
	"github.com/olekukonko/tablewriter"
	log "github.com/sirupsen/logrus"

	constants "github.com/cvmfs/ducc/constants"
	cvmfs "github.com/cvmfs/ducc/cvmfs"
	da "github.com/cvmfs/ducc/docker-api"
	l "github.com/cvmfs/ducc/log"
	notification "github.com/cvmfs/ducc/notification"
)

type ManifestRequest struct {
	Image    Image
	Password string
}

type Image struct {
	Id          int
	User        string
	Scheme      string
	Registry    string
	Repository  string
	Tag         string
	Digest      string
	IsThin      bool
	TagWildcard bool
	Manifest    *da.Manifest
	OCIImage    *image.Image
}

type Credentials struct {
	username string
	password string
}

type RegistryConfig struct {
	baseUrl string
	proxy   string
	creds   Credentials
}

var inputRegistries []RegistryConfig

func SetupRegistries() {
	regs := os.Getenv("DUCC_AUTH_REGISTRIES")
	for _, r := range strings.Split(regs, ",") {
		if r == "" {
			continue
		}

		iEnv := "DUCC_" + r + "_IDENT"
		uEnv := "DUCC_" + r + "_USER"
		uPass := "DUCC_" + r + "_PASS"
		proxyEnv := "DUCC_" + r + "_PROXY"
		ident := os.Getenv(iEnv)
		user := os.Getenv(uEnv)
		pass := os.Getenv(uPass)
		proxy := os.Getenv(proxyEnv)

		if ident == "" || ((user == "" || pass == "") && proxy == "") {
			log.Fatalf("missing either $%s, ($%s or $%s) or %s for %s",
				iEnv, uEnv, uPass, proxyEnv, r)
		}

		inputRegistries = append(inputRegistries, RegistryConfig{
			ident,
			proxy,
			Credentials{user, pass},
		})
	}
}

func (i *Image) GetSimpleName() string {
	name := fmt.Sprintf("%s/%s", i.Registry, i.Repository)
	if i.Tag == "" {
		return name
	} else {
		return name + ":" + i.Tag
	}
}

func (i *Image) WholeName() string {
	root := fmt.Sprintf("%s://%s/%s", i.Scheme, i.Registry, i.Repository)
	if i.Tag != "" {
		root = fmt.Sprintf("%s:%s", root, i.Tag)
	}
	if i.Digest != "" {
		root = fmt.Sprintf("%s@%s", root, i.Digest)
	}
	return root
}

func (i *Image) GetManifestUrl(reference string) string {
	url := i.baseUrl() + "manifests/"
	if reference != "" {
		url = fmt.Sprintf("%s%s", url, reference)
	} else if i.Digest != "" {
		url = fmt.Sprintf("%s%s", url, i.Digest)
	} else {
		url = fmt.Sprintf("%s%s", url, i.Tag)
	}
	return url
}

func (i *Image) GetReference() string {
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

func (i *Image) GetSimpleReference() string {
	if i.Tag != "" {
		return i.Tag
	}
	if i.Digest != "" {
		return i.Digest
	}
	panic("Image wrong format, missing both tag and digest")
}

func (img *Image) PrintImage(machineFriendly, csv_header bool) {
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

func (img *Image) fetchManifest() (*da.Manifest, error) {
	bytes, err := img.getByteManifest("")
	if err != nil {
		return nil, err
	}
	var manifest da.Manifest
	err = json.Unmarshal(bytes, &manifest)
	if err != nil {
		return nil, err
	}
	if reflect.DeepEqual(da.Manifest{}, manifest) {
		return nil, fmt.Errorf("got empty manifest")
	}

	img.Manifest = &manifest
	return &manifest, nil
}

func (img *Image) fetchManifestList() (*da.Manifest, error) {
	bytes1, err := img.getByteManifestList()
	if err != nil {
		return nil, err
	}

	var manifestList da.ManifestList
	err = json.Unmarshal(bytes1, &manifestList)
	if err != nil {
		return nil, err
	}
	if reflect.DeepEqual(da.ManifestList{}, manifestList) {
		return nil, fmt.Errorf("got empty manifest list")
	}

	var manifestReference string
	if len(manifestList.Manifests) == 1 {
		manifestReference = manifestList.Manifests[0].Digest
	} else {
		// TODO: In case of a manifest list with multiple architectures, default to amd64
		// TODO: Support multi-arch images
		for _, v := range manifestList.Manifests {
			if v.Platform.Architecture == "amd64" {
				manifestReference = v.Digest
			}
		}
	}

	bytes2, err := img.getByteManifest(manifestReference)
	if err != nil {
		return nil, err
	}

	var manifest da.Manifest
	err = json.Unmarshal(bytes2, &manifest)
	if err != nil {
		return nil, err
	}
	if reflect.DeepEqual(da.Manifest{}, manifest) {
		return nil, fmt.Errorf("got empty manifest")
	}

	img.Manifest = &manifest
	return &manifest, nil
}

func (img *Image) GetManifest() (da.Manifest, error) {
	if img.Manifest != nil {
		return *img.Manifest, nil
	}

	// First try to fetch a simple manifest
	manifest, err := img.fetchManifest()
	if err != nil || manifest.MediaType == "application/vnd.docker.distribution.manifest.list.v2+json" || manifest.MediaType == "application/vnd.oci.image.index.v1+json" {
		// If the first fetch fails, try to fetch from a manifest list
		manifest, err := img.fetchManifestList()
		if err != nil {
			return da.Manifest{}, fmt.Errorf("could not retrieve manifest for %s", img.WholeName())
		}
		return *manifest, nil
	}

	return *manifest, nil
}

func (img *Image) GetOCIImage() (config image.Image, err error) {
	if img.OCIImage != nil {
		return *img.OCIImage, nil
	}

	manifest, err := img.GetManifest()
	if err != nil {
		l.LogE(err).Warning("Impossible to retrieve the manifest of the image, not changes set")
		return
	}
	configUrl := fmt.Sprintf("%sblobs/%s", img.GetBaseUrl(), manifest.Config.Digest)
	token, err := firstRequestForAuth(configUrl)
	if err != nil {
		l.LogE(err).Warning("Impossible to retrieve the token for getting the changes from the repository, not changes set")
		return
	}
	client := &http.Client{}
	req, err := http.NewRequest("GET", configUrl, nil)
	if err != nil {
		l.LogE(err).Warning("Impossible to create a request for getting the changes no chnages set.")
		return
	}
	req.Header.Set("Authorization", token)
	req.Header.Set("Accept", "application/vnd.docker.distribution.manifest.v2+json")
	req.Header.Set("Accept", "application/vnd.oci.image.manifest.v1+json")

	resp, err := client.Do(req)
	if err != nil {
		l.LogE(err).Warning("error making HTTP request")
		return
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		l.LogE(err).Warning("Error in reading the body from the configuration, no change set")
		return
	}

	err = json.Unmarshal(body, &config)
	if err != nil {
		l.LogE(err).Warning("Error in unmarshaling the configuration of the image")
		return
	}
	img.OCIImage = &config
	return
}

func (img *Image) GetChanges() (changes []string, err error) {
	changes = []string{"ENV CVMFS_IMAGE true"}

	config, err := img.GetOCIImage()
	if err != nil {
		l.LogE(err).Warning("Error in getting configuration of the image")
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
		command := "CMD"
		for _, c := range cmd {
			command = fmt.Sprintf("%s %s", command, c)
		}
		changes = append(changes, command)
	}

	return
}

func (img *Image) GetSingularityLocation() string {
	return fmt.Sprintf("docker://%s/%s%s", img.Registry, img.Repository, img.GetReference())
}

func (img *Image) GetTagListUrl() string {
	return img.baseUrl() + "tags/list"
}

func (img *Image) ExpandWildcard() (<-chan *Image, <-chan *Image, error) {
	r1 := make(chan *Image, 500)
	r2 := make(chan *Image, 500)
	var wg sync.WaitGroup
	defer func() {
		go func() {
			wg.Wait()
			close(r1)
			close(r2)
		}()
	}()
	if !img.TagWildcard {
		img.GetManifest()
		r1 <- img
		r2 <- img
		return r1, r2, nil
	}
	var tagsList struct {
		Tags []string
	}
	url := img.GetTagListUrl()
	token, err := firstRequestForAuth(url)
	if err != nil {
		errF := fmt.Errorf("error in authenticating for retrieving the tags: %s", err)
		l.LogE(err).Error(errF)
		return r1, r2, errF
	}

	client := http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", token)

	resp, err := client.Do(req)
	if err != nil {
		errF := fmt.Errorf("error making the request for retrieving the tags: %s", err)
		l.LogE(err).WithFields(log.Fields{"url": url}).Error(errF)
		return r1, r2, errF
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		errF := fmt.Errorf("error status code (%d) trying to retrieve the tags", resp.StatusCode)
		l.LogE(err).WithFields(log.Fields{"status code": resp.StatusCode, "url": url}).Error(errF)
		return r1, r2, errF
	}
	if err = json.NewDecoder(resp.Body).Decode(&tagsList); err != nil {
		errF := fmt.Errorf("error in decoding the tags from the server: %s", err)
		l.LogE(err).Error(errF)
		return r1, r2, errF
	}
	pattern := img.Tag
	filteredTags, err := filterUsingGlob(pattern, tagsList.Tags)
	if err != nil {
		return r1, r2, nil
	}

	tagChan := make(chan string, 40)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, tag := range filteredTags {
			tagChan <- tag
		}
		close(tagChan)
	}()

	for worker := 0; worker <= 20; worker += 1 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for tag := range tagChan {
				taggedImg := *img
				taggedImg.Tag = tag
				taggedImg.GetManifest()
				r1 <- &taggedImg
				r2 <- &taggedImg
			}
		}()
	}

	return r1, r2, nil
}

func filterUsingGlob(pattern string, toFilter []string) ([]string, error) {
	result := make([]string, 0)
	regexPattern := strings.ReplaceAll(pattern, "*", ".*")
	regex, err := regexp.Compile(regexPattern)
	if err != nil {
		return result, err
	}
	regex.Longest()
	for _, toCheck := range toFilter {
		s := regex.FindString(toCheck)
		if s == "" {
			continue
		}
		if s == toCheck {
			result = append(result, s)
		}
	}
	return result, nil
}

// here is where in the FS we are going to store the singularity image
func (img *Image) GetSingularityPath() (string, error) {
	manifest, err := img.GetManifest()
	if err != nil {
		l.LogE(err).Error("Error in getting the manifest to figureout the singularity path")
		return "", err
	}
	return manifest.GetSingularityPath(), nil
}

// the one that the user see, without the /cvmfs/$repo.cern.ch prefix
// used mostly by Singularity
func (i *Image) GetPublicSymlinkPath() string {
	return filepath.Join(i.Registry, i.Repository+":"+i.GetSimpleReference())
}

func (img *Image) getByteManifestList() ([]byte, error) {
	url := img.GetManifestUrl("")
	return makeGetRequest(url, map[string]string{"Accept": "application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.index.v1+json"})
}

func (img *Image) getByteManifest(reference string) ([]byte, error) {
	url := img.GetManifestUrl(reference)
	return makeGetRequest(url, map[string]string{"Accept": "application/vnd.docker.distribution.manifest.v2+json, application/vnd.oci.image.manifest.v1+json"})
}

func GetAuthToken(url string, credentials []Credentials) (token string, err error) {
	reg := getRegistry(url)
	if reg != nil && reg.proxy == "" {
		return firstRequestForAuth_internal(url, reg.creds.username, reg.creds.password)
	}
	return firstRequestForAuth_internal(url, "", "")
}

func firstRequestForAuth(url string) (token string, err error) {
	credentials := []Credentials{}
	return GetAuthToken(url, credentials)
}

func firstRequestForAuth_internal(url, user, pass string) (token string, err error) {
	resp, err := http.Get(url)
	if err != nil {
		l.LogE(err).Error("Error in making the first request for auth")
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 300 && resp.StatusCode >= 200 {
		log.WithFields(log.Fields{
			"status code": resp.StatusCode,
		}).Info("Return valid response, token not necessary.")
		return
	}
	if resp.StatusCode != 401 {
		log.WithFields(log.Fields{
			"url":         url,
			"status code": resp.StatusCode,
		}).Info("Expected status code 401.")
		return "", err
	}
	WwwAuthenticate := resp.Header["Www-Authenticate"][0]
	// we first try to get the token with the authentication
	// if we fail, and we might since the docker hub might not have our user
	// we try again without authentication
	token, err = requestAuthToken(WwwAuthenticate, user, pass)
	if err == nil {
		// happy path
		return token, nil
	}
	fmt.Printf("We failed with authentication and we now go without for %s\n", url)
	// some error, we should retry without auth
	if user != "" || pass != "" {
		token, err = requestAuthToken(WwwAuthenticate, "", "")
		if err == nil {
			// happy path without auth
			return token, nil
		}
	}
	l.LogE(err).Error("Error in getting the authentication token")
	return "", err
}

func getLayerUrl(img *Image, layerDigest string) string {
	return fmt.Sprintf("%sblobs/%s", img.baseUrl(), layerDigest)
}

type downloadedLayer struct {
	Name string
	Path ReadHashCloseSizer
}

func newDownloadedLayer(name string, path ReadHashCloseSizer) downloadedLayer {
	return downloadedLayer{Name: name, Path: path}
}

func (d *downloadedLayer) Close() error {
	// sometimes we might be forced to return the zero value of downloadedLayer
	// in that case Path will point to nil
	if d.Path != nil {
		return d.Path.Close()
	}
	return nil
}

func (d *downloadedLayer) IngestIntoCVMFS(CVMFSRepo string) error {
	layerDigest := strings.Split(d.Name, ":")[1]
	layerPath := cvmfs.LayerRootfsPath(CVMFSRepo, layerDigest)
	if _, err := os.Stat(layerPath); err == nil {
		// the layer already exists
		return nil
	}
	superDir := filepath.Dir(filepath.Dir(cvmfs.TrimCVMFSRepoPrefix(layerPath)))
	go cvmfs.CreateCatalogIntoDir(CVMFSRepo, superDir)
	ingestPath := cvmfs.TrimCVMFSRepoPrefix(layerPath)
	err := cvmfs.Ingest(CVMFSRepo, d.Path,
		"--catalog", "-t", "-",
		"-b", ingestPath)
	if err != nil {
		l.LogE(err).WithFields(
			log.Fields{"layer": d.Name}).
			Error("Some error in ingest the layer")
		go cvmfs.IngestDelete(CVMFSRepo, ingestPath)
		return err
	}
	err = StoreLayerInfo(CVMFSRepo, layerDigest, d.Path)
	if err != nil {
		return err
	}
	return nil
}

// only accurate at the END
func (d *downloadedLayer) GetSize() int64 {
	if d.Path != nil {
		return d.Path.GetSize()
	}
	return 0
}

func (img *Image) GetLayers(layersChan chan<- downloadedLayer, manifestChan chan<- string, stopGettingLayers <-chan bool, rootPath string) error {
	defer close(layersChan)
	defer close(manifestChan)

	layerDownloader := NewLayerDownloader(img)
	_, err := layerDownloader.getToken()
	if err != nil {
		return err
	}

	// then we try to get the manifest from our database
	manifest, err := img.GetManifest()
	if err != nil {
		l.LogE(err).Warn("Error in getting the manifest")
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
			err := fmt.Errorf("detect errors, stop getting layer")
			errorChannel <- err
			l.LogE(err).Error("Detect error, stop getting layers")
			cancel()
			return
		}
	}()
	defer func() { killKiller <- true }()

	var wg sync.WaitGroup
	defer wg.Wait()
	// at this point we iterate each layer and we download it.
	for _, layer := range manifest.Layers {
		if layer.MediaType == "application/vnd.docker.image.rootfs.foreign.diff.tar.gzip" {
			continue
		}

		wg.Add(1)
		go func(ctx context.Context, layer da.Layer) {

			defer wg.Done()

			l.Log().WithFields(
				log.Fields{"layer": layer.Digest}).
				Info("Start working on layer")

			toSend, err := layerDownloader.DownloadLayer(layer)

			if err != nil {
				l.LogE(err).Error("Error in downloading a layer")
				toSend.Close()
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
		l.LogE(err).Error("Error in marshaling the manifest")
		return err
	}
	manifestPath := filepath.Join(rootPath, "manifest.json")
	err = ioutil.WriteFile(manifestPath, manifestBytes, 0666)
	if err != nil {
		l.LogE(err).Error("Error in writing the manifest to file")
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

func (img *Image) downloadLayer(layer da.Layer, token string) (toSend downloadedLayer, err error) {
	layerUrl := getLayerUrl(img, layer.Digest)
	if token == "" {
		token, err = firstRequestForAuth(layerUrl)
		if err != nil {
			return
		}
	}
	for i := 0; i <= 5; i++ {
		err = nil
		client := &http.Client{}
		req, errR := http.NewRequest("GET", layerUrl, nil)
		if err != nil {
			l.LogE(errR).Error("Impossible to create the HTTP request.")
			err = errR
			break
		}
		req.Header.Set("Authorization", token)
		resp, errReq := client.Do(req)
		l.Log().WithFields(
			log.Fields{"layer": layer.Digest, "size in MB": (layer.Size / 1e6)}).
			Info("Make request for layer")
		if errReq != nil {
			err = errReq
			break
		}
		if 200 <= resp.StatusCode && resp.StatusCode < 300 {
			gread, errG := gzip.NewReader(resp.Body)
			if errG != nil {
				err = errG
				l.LogE(err).Warning("Error in creating the zip to unzip the layer")
				continue
			}
			path := NewReadAndHash(gread)
			toSend = newDownloadedLayer(layer.Digest, path)
			return toSend, nil
		} else {
			err = fmt.Errorf("layer not received, status code: %d", resp.StatusCode)
			l.LogE(err).Warning("Received status code ", resp.StatusCode)
			if resp.StatusCode == 401 {
				// try to get the token again
				newToken, errToken := firstRequestForAuth(layerUrl)
				if errToken != nil {
					l.LogE(errToken).Warning("Error in refreshing the token")
				} else {
					token = newToken
				}
			}
		}
	}
	l.LogE(err).Warning("return from error path")
	return
}

func parseBearerToken(token string) (realm string, options map[string]string, err error) {
	options = make(map[string]string)
	args := token[7:]
	keyValue := strings.Split(args, ",")
	for _, kv := range keyValue {
		splitted := strings.Split(kv, "=")
		if len(splitted) != 2 {
			err = fmt.Errorf("wrong formatting of the token")
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
	if err != nil {
		err = fmt.Errorf("error in getting the token, http request failed %s", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		err = fmt.Errorf("authorization error %s", resp.Status)
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
		err = fmt.Errorf("didn't get the token key from the server")
		return
	}
	return
}

type LayerDownloader struct {
	image    *Image
	token    string
	attempts map[string]int
	lock     sync.Mutex
}

func NewLayerDownloader(image *Image) LayerDownloader {
	return LayerDownloader{image: image, token: "", attempts: make(map[string]int)}
}

func (ld *LayerDownloader) getToken() (token string, err error) {
	ld.lock.Lock()
	defer ld.lock.Unlock()
	if ld.token != "" {
		return ld.token, nil
	}
	manifest, err := ld.image.GetManifest()
	if err != nil {
		return
	}

	firstLayer := manifest.Layers[0]
	for _, l := range manifest.Layers {
		if l.MediaType == "application/vnd.docker.image.rootfs.foreign.diff.tar.gzip" {
			continue
		}
		firstLayer = l
		break
	}
	layerUrl := getLayerUrl(ld.image, firstLayer.Digest)
	token, err = firstRequestForAuth(layerUrl)
	if err != nil {
		return
	}
	ld.token = token
	return
}

func (ld *LayerDownloader) DownloadLayer(layer da.Layer) (downloadedLayer, error) {
	token, err := ld.getToken()
	if err != nil {
		return downloadedLayer{}, err
	}
	ld.lock.Lock()
	att := ld.attempts[layer.Digest]
	ld.attempts[layer.Digest] = (att + 1)
	ld.lock.Unlock()

	// if the layer is bigger than 50M we download it using the disk storage
	if att == 0 && layer.Size < 50e6 {
		// in this case it is smaller and we do an early exit
		return ld.image.downloadLayer(layer, token)
	}
	inMem, err := ld.image.downloadLayer(layer, token)
	if err != nil {
		return inMem, err
	}
	r, err := NewDiskBufferReadAndHash(inMem.Path)
	if err != nil {
		return inMem, err
	}
	return newDownloadedLayer(inMem.Name, r), nil
}

func (ld *LayerDownloader) DownloadAndIngest(CVMFSRepo string, layer da.Layer) error {
	err := error(nil)
	for i := 0; i <= 5; i += 1 {
		to_ingest, err := ld.DownloadLayer(layer)
		if err != nil {
			// let's try again
			continue
		}
		defer to_ingest.Close()
		err = to_ingest.IngestIntoCVMFS(CVMFSRepo)
		if err == nil {
			return nil
		}
	}
	return err
}

func (img *Image) CreateSneakyChainStructure(CVMFSRepo string) (err error, lastChainId string) {
	// make sure we have the layers somewhere
	manifest, err := img.GetManifest()
	if err != nil {
		return
	}

	// then we start creating the chain structure
	chainIDs := manifest.GetChainIDs()

	paths := []string{}
	for _, chain := range chainIDs {
		if chain == "" {
			continue
		}
		path := cvmfs.ChainPath(CVMFSRepo, chain.String())
		dir := filepath.Dir(path)
		if _, err := os.Stat(dir); err != nil {
			paths = append(paths, dir)
		}
	}

	if len(paths) > 0 {
		err = cvmfs.WithinTransaction(CVMFSRepo, func() error {
			for _, dir := range paths {
				if err := os.MkdirAll(dir, constants.DirPermision); err != nil {
					return err
				}
				// create the .cvmfscatalog, we don't really care if it fails
				f, _ := os.OpenFile(filepath.Join(dir, ".cvmfscatalog"),
					os.O_CREATE|os.O_RDONLY, constants.FilePermision)
				f.Close()
			}
			return nil
		})
		if err != nil {
			l.LogE(err).Error("Impossible to create directory to contains the chainID")
			return
		}
	}

	dirtyChains := cvmfs.GetDirtyChains(CVMFSRepo)
	if len(dirtyChains) > 0 {
		err = cvmfs.WithinTransaction(CVMFSRepo, func() error {
			for _, chain := range dirtyChains {
				chainPath := cvmfs.ChainPath(CVMFSRepo, chain)
				dirtyChainPath := cvmfs.DirtyChainPath(CVMFSRepo, chain)
				if err := os.RemoveAll(chainPath); err != nil {
					return err
				}
				if err := os.RemoveAll(dirtyChainPath); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			l.LogE(err).Error("Error in deleting dirty chains, unsafe to continue")
			return
		}
	}

	n := notification.NewNotification(NotificationService)
	n = n.AddField("image", img.GetSimpleName())

	n.AddField("action", "start_chains_ingestion").Send()

	ld := NewLayerDownloader(img)
	previous := ""
	for i, chain := range chainIDs {
		if chain == "" {
			continue
		}
		digest := chain.String()
		lastChainId = digest
		layer := manifest.Layers[i]

		l.Log().WithFields(
			log.Fields{"chain id": digest, "next layer": layer.Digest}).
			Info("adding new chain")

		path := cvmfs.ChainPath(CVMFSRepo, digest)

		if _, err := os.Stat(path); err == nil {
			// the chain is present, we skip the loop
			l.Log().WithFields(log.Fields{"chain id": digest}).Info("skipping (already present)")
			previous = chainIDs[i].String()
			continue
		}

		downloadLayer := func() error {
			// we need to get the layer tar reader here
			layerStream, err := ld.DownloadLayer(layer)

			// we should call this even if there were issues in creating the file
			defer layerStream.Close()

			if err != nil {
				l.LogE(err).Error("Error in downloading the layer from the docker registry")
				return err
			}

			tarReader := *tar.NewReader(layerStream.Path)

			chainN := n.AddField("chain", chain.String()).
				AddField("layer", strings.Split(layer.Digest, ":")[1]).
				AddId()

			t := time.Now()
			chainN.Action("start_single_chain_ingestion").Send()

			err = cvmfs.CreateSneakyChain(CVMFSRepo,
				chain.String(),
				previous,
				tarReader)

			chainN.Elapsed(t).
				Action("end_single_chain_ingestion").
				SizeBytes(layerStream.GetSize()).
				Error(err).
				Send()

			return err
		}
		for attempt := 0; attempt < 5; attempt++ {
			l.Log().Info("Start attempt: ", attempt)
			err = downloadLayer()
			if err == nil {
				l.Log().Info("Attempt ", attempt, " success")
				break
			}
			l.Log().Warn("Attempt ", attempt, " fail")
		}
		if err != nil {
			l.LogE(err).Error("Error in creating the chain")
			return err, lastChainId
		}
		previous = chainIDs[i].String()
	}
	return
}

func getRegistry(url string) *RegistryConfig {
	for _, reg := range inputRegistries {
		if strings.Contains(url, reg.baseUrl) {
			return &reg
		}
	}
	return nil
}

func (i *Image) baseUrl() string {
	var url string
	reg := getRegistry(i.Registry)
	if reg != nil && reg.proxy != "" {
		proxyHost, proxyPath, found := strings.Cut(reg.proxy, "/")
		if found {
			url = fmt.Sprintf("%s://%s/v2/%s/%s/", i.Scheme, proxyHost, proxyPath, i.Repository)
		} else {
			url = fmt.Sprintf("%s://%s/v2/%s/", i.Scheme, proxyHost, i.Repository)
		}
	} else {
		url = fmt.Sprintf("%s://%s/v2/%s/", i.Scheme, i.Registry, i.Repository)
	}
	return url
}

func (i *Image) GetBaseUrl() string {
	return i.baseUrl()
}

func makeGetRequest(url string, headers map[string]string) ([]byte, error) {
	token, err := firstRequestForAuth(url)
	if err != nil {
		l.LogE(err).Error("Error in getting the authentication token")
		return nil, err
	}

	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		l.LogE(err).Error("Impossible to create a HTTP request")
		return nil, err
	}

	req.Header.Set("Authorization", token)
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := client.Do(req)
	if err != nil {
		l.LogE(err).Error("Error in making the HTTP request")
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		l.LogE(err).Error("Error in reading the second http response")
		return nil, err
	}

	return body, nil
}
