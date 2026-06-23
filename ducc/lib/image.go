package lib

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aoliveti/curling"
	"github.com/docker/docker/image"
	"github.com/olekukonko/tablewriter"
	log "github.com/sirupsen/logrus"

	cvmfs "github.com/cvmfs/ducc/cvmfs"
	da "github.com/cvmfs/ducc/docker-api"
	l "github.com/cvmfs/ducc/log"
	notification "github.com/cvmfs/ducc/notification"
)

var (
	logCurlReqOnce    sync.Once
	logCurlReqEnabled bool
)

func logCurlEnabled() bool {
	logCurlReqOnce.Do(func() {
		logCurlReqEnabled = os.Getenv("DUCC_DEBUG_LOG_CURL_REQ") != ""
	})
	return logCurlReqEnabled
}

type ManifestRequest struct {
	Image    Image
	Password string
}

type Image struct {
	Id               int
	User             string
	Scheme           string
	Registry         string
	Repository       string
	Tag              string
	Digest           string
	IsThin           bool
	TagWildcard      bool
	Manifest         *da.Manifest
	OCIImage         *image.Image
	ManifestList     *da.ManifestList
	rawManifestBytes []byte // cached raw wire bytes of the resolved manifest
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

// Token cache to avoid redundant auth requests for the same registry/repository.
type cachedToken struct {
	token     string
	expiresAt time.Time
}

var (
	tokenCacheMu  sync.RWMutex
	tokenCacheMap = make(map[string]*cachedToken)
)

// extractTokenCacheKey extracts a repository-scoped cache key from a registry URL.
// URLs like https://registry/v2/repo/manifests/ref and https://registry/v2/repo/blobs/digest
// all map to the same cache key based on the registry+repository prefix.
func extractTokenCacheKey(url, user string) string {
	for _, suffix := range []string{"/manifests/", "/blobs/", "/tags/"} {
		if idx := strings.Index(url, suffix); idx != -1 {
			return url[:idx] + "|" + user
		}
	}
	return url + "|" + user
}

func getCachedToken(key string) (string, bool) {
	tokenCacheMu.RLock()
	defer tokenCacheMu.RUnlock()
	cached, ok := tokenCacheMap[key]
	if !ok {
		return "", false
	}
	if time.Now().After(cached.expiresAt) {
		return "", false
	}
	return cached.token, true
}

func setCachedToken(key, token string, expiresIn int) {
	tokenCacheMu.Lock()
	defer tokenCacheMu.Unlock()
	ttl := time.Duration(expiresIn) * time.Second
	if ttl <= 0 {
		ttl = 60 * time.Second // default 60s TTL
	}
	// Apply safety margin: use 80% of the TTL to refresh before actual expiry
	ttl = time.Duration(float64(ttl) * 0.8)
	tokenCacheMap[key] = &cachedToken{
		token:     token,
		expiresAt: time.Now().Add(ttl),
	}
}

const rateLimitMaxRetries = 5

// handleRateLimitBackoff checks if the response is a 429 (Too Many Requests) and implements
// exponential backoff with jitter. Returns true if a retry should be attempted.
func handleRateLimitBackoff(resp *http.Response, attempt int, maxRetries int) bool {
	if resp.StatusCode != 429 {
		return false
	}

	if attempt >= maxRetries {
		l.Log().WithFields(log.Fields{
			"attempt":     attempt,
			"status_code": resp.StatusCode,
		}).Warning("Max retries reached for 429 response")
		return false
	}

	// Check for Retry-After header
	var waitDuration time.Duration
	if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
		// Try to parse as seconds (integer)
		if seconds, err := strconv.Atoi(retryAfter); err == nil {
			waitDuration = time.Duration(seconds) * time.Second
		} else {
			// Try to parse as HTTP date
			if retryTime, err := time.Parse(time.RFC1123, retryAfter); err == nil {
				waitDuration = time.Until(retryTime)
				if waitDuration < 0 {
					waitDuration = 0
				}
			}
		}
	}

	// If no Retry-After header or parsing failed, use exponential backoff
	if waitDuration == 0 {
		// Exponential backoff: 2^attempt seconds, with a max of 60 seconds
		backoffSeconds := math.Min(math.Pow(2, float64(attempt)), 60)
		waitDuration = time.Duration(backoffSeconds) * time.Second
	}

	// Add jitter (±25% randomization)
	jitter := time.Duration(float64(waitDuration) * 0.25 * (2*rand.Float64() - 1))
	waitDuration += jitter

	l.Log().WithFields(log.Fields{
		"attempt":       attempt,
		"wait_duration": waitDuration,
		"status_code":   resp.StatusCode,
	}).Info("Rate limited by registry (429), backing off before retry")

	time.Sleep(waitDuration)
	return true
}

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

func (img *Image) FetchManifestList2() (*da.ManifestList, error) {
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

	var validIndex []int
	var manifestReference string
	if len(manifestList.Manifests) == 1 {
		manifestReference = manifestList.Manifests[0].Digest
	} else {

		for i, v := range manifestList.Manifests {
			if v.Platform.Architecture == "amd64" {
				manifestReference = v.Digest

			}
			// skip "unknown" architecture
			if v.Platform.Architecture != "unknown" {
				validIndex = append(validIndex, i)

			}
		}
	}

	manifestsFiltered := make([]da.ManifestListItem, 0)
	for _, j := range validIndex {
		manifestsFiltered = append(manifestsFiltered, manifestList.Manifests[j])
	}
	manifestList.Manifests = manifestsFiltered

	for i, v := range manifestList.Manifests {
		bytes2, err := img.getByteManifest(v.Digest)
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
		manifestList.Manifests[i].Manifest = manifest
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
	return &manifestList, nil
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
	img.rawManifestBytes = bytes
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
	img.rawManifestBytes = bytes2
	return &manifest, nil
}

func (img *Image) GetManifestList() (da.ManifestList, error) {
	if img.ManifestList != nil {
		return *img.ManifestList, nil
	}

	var manifestList da.ManifestList
	// First try to fetch a simple manifest
	manifest, err := img.fetchManifest()

	if err != nil || manifest.MediaType == "application/vnd.docker.distribution.manifest.list.v2+json" || manifest.MediaType == "application/vnd.oci.image.index.v1+json" {
		// If the first fetch fails, try to fetch from a manifest list
		manifestList2, err := img.FetchManifestList2()
		if err != nil {
			return da.ManifestList{}, fmt.Errorf("could not retrieve manifestlist for %s", img.WholeName())
		}
		return *manifestList2, nil
	} else if err == nil {
		var placeholderitem da.ManifestListItem
		placeholderitem.Manifest = *manifest
		placeholderitem.Platform.Architecture = "" //for images without manifestlist, assume amd64 arch
		manifestList.Manifests = append(manifestList.Manifests, placeholderitem)
		manifestList.MediaType = "SingleManifest"

	}

	return manifestList, nil
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
		l.LogE(err).Warning("Impossible to create a request for getting the changes, no changes set.")
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

	for attempt := 0; attempt <= rateLimitMaxRetries; attempt++ {
		req, reqErr := http.NewRequest("GET", url, nil)
		if reqErr != nil {
			errF := fmt.Errorf("error creating the request for retrieving the tags: %s", reqErr)
			l.LogE(reqErr).WithFields(log.Fields{"url": url}).Error(errF)
			return r1, r2, errF
		}
		req.Header.Set("Authorization", token)

		resp, respErr := client.Do(req)
		if respErr != nil {
			errF := fmt.Errorf("error making the request for retrieving the tags: %s", respErr)
			l.LogE(respErr).WithFields(log.Fields{"url": url}).Error(errF)
			return r1, r2, errF
		}

		// Handle 429 rate limiting
		if handleRateLimitBackoff(resp, attempt, rateLimitMaxRetries) {
			resp.Body.Close()
			continue
		}

		if resp.StatusCode >= 400 {
			errF := fmt.Errorf("error status code (%d) trying to retrieve the tags", resp.StatusCode)
			l.LogE(errF).WithFields(log.Fields{"status code": resp.StatusCode, "url": url}).Error(errF)
			resp.Body.Close()
			return r1, r2, errF
		}
		if err = json.NewDecoder(resp.Body).Decode(&tagsList); err != nil {
			errF := fmt.Errorf("error in decoding the tags from the server: %s", err)
			l.LogE(err).Error(errF)
			resp.Body.Close()
			return r1, r2, errF
		}
		resp.Body.Close()
		// Successfully decoded, break out of retry loop
		break
	}
	if tagsList.Tags == nil {
		return r1, r2, fmt.Errorf("max retries exceeded for rate limiting while retrieving tags")
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

// GetSingularityPath2 returns the singularity path for a given manifest,
// without needing to fetch the manifest from the image.
func (img *Image) GetSingularityPath2(manifest da.Manifest) (string, error) {
	return manifest.GetSingularityPath(), nil
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
func (i *Image) GetPublicSymlinkPathWithArch(arch string) string {
	return filepath.Join(arch, i.Registry, i.Repository+":"+i.GetSimpleReference())
}

// the one that the user see, without the /cvmfs/$repo.cern.ch prefix
// used mostly by Singularity
func (i *Image) GetPublicSymlinkPath() string {
	return filepath.Join(i.Registry, i.Repository+":"+i.GetSimpleReference())
}

// GetVariantSymlinkTarget returns the raw symlink target string for a CVMFS
// variant symlink placed at GetPublicSymlinkPath().  The target embeds a
// $(CVMFS_ARCH:-defaultArch) expression so the CVMFS client resolves it to
// the architecture-specific flat image for the current host at runtime.
//
// For example, if the public path is "docker.io/user/myimage:latest" and
// defaultArch is "amd64", the returned target is:
//
//	../../.multiarch/$(CVMFS_ARCH:-amd64)/docker.io/user/myimage:latest
//
// which, with CVMFS_ARCH=arm64 set in the client config, resolves to
// .multiarch/arm64/docker.io/user/myimage:latest relative to the repo root.
func (i *Image) GetVariantSymlinkTarget(defaultArch string) string {
	publicPath := i.GetPublicSymlinkPath()
	parentDir := filepath.Dir(publicPath)
	// Count the number of directory components between the repo root and the
	// symlink's containing directory; that many "../" steps bring us back to
	// the repo root from the symlink's location.
	depth := len(strings.Split(parentDir, "/"))
	upSteps := strings.Repeat("../", depth)
	varExpr := fmt.Sprintf("$(CVMFS_ARCH:-%s)", defaultArch)
	// Explicit string join to preserve the $(VAR) expression intact.
	return upSteps + ".multiarch/" + varExpr + "/" + publicPath
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
	cacheKey := extractTokenCacheKey(url, user)
	if cached, ok := getCachedToken(cacheKey); ok {
		log.WithFields(log.Fields{"url": url}).Debug("Using cached auth token")
		return cached, nil
	}

	client := &http.Client{}

	for attempt := 0; attempt <= rateLimitMaxRetries; attempt++ {
		req, reqErr := http.NewRequest("GET", url, nil)
		if reqErr != nil {
			l.LogE(reqErr).Error("Error in creating the first request for auth")
			return "", reqErr
		}

		// Advertise support for both Docker v2 and OCI manifest types.
		// Without this, registries that store OCI manifests return 404
		// (MANIFEST_UNKNOWN) instead of 200/401, breaking auth detection.
		req.Header.Set("Accept", "application/vnd.docker.distribution.manifest.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.index.v1+json")

		// for debugging: log curl command corresponding to request
		if logCurlEnabled() {
			curlcmd, curlErr := curling.NewFromRequest(req)
			if curlErr != nil {
				log.Fatal(curlErr)
			}
			log.Debug(curlcmd)
		}

		resp, respErr := client.Do(req)
		if respErr != nil {
			l.LogE(respErr).Error("Error in making the first request for auth")
			return "", respErr
		}

		// Handle 429 rate limiting
		if handleRateLimitBackoff(resp, attempt, rateLimitMaxRetries) {
			resp.Body.Close()
			continue
		}

		if resp.StatusCode < 300 && resp.StatusCode >= 200 {
			log.WithFields(log.Fields{
				"status code": resp.StatusCode,
			}).Info("Return valid response, token not necessary.")
			resp.Body.Close()
			// Cache "no auth needed" with a short TTL
			setCachedToken(cacheKey, "", 60)
			return "", nil
		}
		if resp.StatusCode != 401 {
			log.WithFields(log.Fields{
				"url":         url,
				"status code": resp.StatusCode,
			}).Info("Expected status code 401.")
			resp.Body.Close()
			return "", fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}
		WwwAuthenticate := resp.Header["Www-Authenticate"][0]
		resp.Body.Close()
		// we first try to get the token with the authentication
		// if we fail, and we might since the docker hub might not have our user
		// we try again without authentication
		var expiresIn int
		token, expiresIn, err = requestAuthToken(WwwAuthenticate, user, pass)
		if err == nil {
			// happy path
			setCachedToken(cacheKey, token, expiresIn)
			return token, nil
		}
		fmt.Printf("We failed with authentication and we now go without for %s\n", url)
		// some error, we should retry without auth
		if user != "" || pass != "" {
			token, expiresIn, err = requestAuthToken(WwwAuthenticate, "", "")
			if err == nil {
				// happy path without auth
				setCachedToken(cacheKey, token, expiresIn)
				return token, nil
			}
		}
		l.LogE(err).Error("Error in getting the authentication token")
		return "", err
	}
	return "", fmt.Errorf("max retries exceeded for rate limiting")
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
	return d.IngestIntoCVMFSWithLogger(nil, CVMFSRepo)
}

func (d *downloadedLayer) IngestIntoCVMFSWithLogger(logger *log.Entry, CVMFSRepo string) error {
	logger = l.Ensure(logger)
	layerDigest := strings.Split(d.Name, ":")[1]
	layerPath := cvmfs.LayerRootfsPath(CVMFSRepo, layerDigest)
	if _, err := os.Stat(layerPath); err == nil {
		// the layer already exists
		return nil
	}
	superDir := filepath.Dir(filepath.Dir(cvmfs.TrimCVMFSRepoPrefix(layerPath)))
	if err := cvmfs.CreateCatalogIntoDirWithLogger(logger, CVMFSRepo, superDir); err != nil {
		logger.WithFields(log.Fields{"layer": d.Name, "dir": superDir, "error": err}).
			Error("Error creating catalog for layer directory")
		return err
	}
	ingestPath := cvmfs.TrimCVMFSRepoPrefix(layerPath)

	err := cvmfs.IngestWithLogger(logger, CVMFSRepo, d.Path,
		"--catalog", "-t", "-",
		"--tolerate-missing-hardlinks",
		"-b", ingestPath)
	if err != nil {
		logger.WithFields(log.Fields{"layer": d.Name, "error": err}).
			Error("Some error in ingest the layer")
		if errDelete := cvmfs.IngestDeleteWithLogger(logger, CVMFSRepo, ingestPath); errDelete != nil {
			logger.WithFields(log.Fields{"layer": d.Name, "path": ingestPath, "error": errDelete}).
				Warning("Error cleaning up failed ingest path")
		}
		return err
	}
	err = StoreLayerInfoWithLogger(logger, CVMFSRepo, layerDigest, d.Path)
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

func (img *Image) GetLayers(manifest da.Manifest, layersChan chan<- downloadedLayer, manifestChan chan<- string, stopGettingLayers <-chan bool, rootPath string, maxConcurrentDownloads int, CVMFSRepo string, forceDownload bool) error {
	return img.GetLayersWithLogger(nil, manifest, layersChan, manifestChan, stopGettingLayers, rootPath, maxConcurrentDownloads, CVMFSRepo, forceDownload)
}

func (img *Image) GetLayersWithLogger(logger *log.Entry, manifest da.Manifest, layersChan chan<- downloadedLayer, manifestChan chan<- string, stopGettingLayers <-chan bool, rootPath string, maxConcurrentDownloads int, CVMFSRepo string, forceDownload bool) error {
	logger = l.Ensure(logger)
	defer close(layersChan)
	defer close(manifestChan)

	layerDownloader := NewLayerDownloaderWithLogger(logger, img)
	_, err := layerDownloader.getToken()
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
			err := fmt.Errorf("detect errors, stop getting layer")
			errorChannel <- err
			logger.WithField("error", err).Error("Detect error, stop getting layers")
			cancel()
			return
		}
	}()
	defer func() { killKiller <- true }()

	// Use a semaphore to limit concurrent layer downloads if maxConcurrentDownloads > 0.
	// A zero or negative value means unlimited concurrency.
	var sem chan struct{}
	if maxConcurrentDownloads > 0 {
		sem = make(chan struct{}, maxConcurrentDownloads)
	}

	var wg sync.WaitGroup
	defer wg.Wait()
	// at this point we iterate each layer and we download it.
	for _, layer := range manifest.Layers {
		if layer.MediaType == "application/vnd.docker.image.rootfs.foreign.diff.tar.gzip" {
			continue
		}

		if sem != nil {
			sem <- struct{}{}
		}

		wg.Add(1)
		go func(ctx context.Context, layer da.Layer) {
			if sem != nil {
				defer func() { <-sem }()
			}
			defer wg.Done()

			layerDigest := strings.Split(layer.Digest, ":")[1]
			layerPath := cvmfs.LayerRootfsPath(CVMFSRepo, layerDigest)
			layerLogger := logger.WithField("layer", layer.Digest)
			if !forceDownload {
				if _, err := os.Stat(layerPath); err == nil {
					layerLogger.Trace("Skipping download of layer, already exists")
					return
				}
			}

			layerLogger.Trace("Start working on layer")

			toSend, err := layerDownloader.DownloadLayer(layer)

			if err != nil {
				layerLogger.WithField("error", err).Error("Error in downloading a layer")
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
		logger.WithField("error", err).Error("Error in marshaling the manifest")
		return err
	}
	manifestPath := filepath.Join(rootPath, "manifest.json")
	err = ioutil.WriteFile(manifestPath, manifestBytes, 0666)
	if err != nil {
		logger.WithField("error", err).Error("Error in writing the manifest to file")
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
	return img.downloadLayerWithLogger(nil, layer, token)
}

func (img *Image) downloadLayerWithLogger(logger *log.Entry, layer da.Layer, token string) (toSend downloadedLayer, err error) {
	logger = l.Ensure(logger)
	layerUrl := getLayerUrl(img, layer.Digest)
	if token == "" {
		token, err = firstRequestForAuth(layerUrl)
		if err != nil {
			return
		}
	}
	for i := 0; i <= rateLimitMaxRetries; i++ {
		err = nil
		client := &http.Client{}
		req, errR := http.NewRequest("GET", layerUrl, nil)
		if errR != nil {
			logger.WithField("error", errR).Error("Impossible to create the HTTP request")
			err = errR
			break
		}
		req.Header.Set("Authorization", token)
		resp, errReq := client.Do(req)
		logger.WithField("size in MB", (layer.Size / 1e6)).Trace("Make request for layer")
		if errReq != nil {
			err = errReq
			break
		}

		// Handle 429 rate limiting
		if handleRateLimitBackoff(resp, i, rateLimitMaxRetries) {
			resp.Body.Close()
			continue
		}

		if 200 <= resp.StatusCode && resp.StatusCode < 300 {
			gread, errG := gzip.NewReader(resp.Body)
			if errG != nil {
				err = errG
				logger.WithField("error", err).Warning("Error in creating the zip to unzip the layer")
				resp.Body.Close()
				continue
			}
			path := NewReadAndHash(gread)
			toSend = newDownloadedLayer(layer.Digest, path)
			return toSend, nil
		} else {
			err = fmt.Errorf("layer not received, status code: %d", resp.StatusCode)
			logger.WithField("error", err).Warnf("Received status code %d", resp.StatusCode)
			resp.Body.Close()
			if resp.StatusCode == 401 {
				// try to get the token again
				newToken, errToken := firstRequestForAuth(layerUrl)
				if errToken != nil {
					logger.WithField("error", errToken).Warning("Error in refreshing the token")
				} else {
					token = newToken
				}
			}
		}
	}
	logger.WithField("error", err).Warning("Return from error path")
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

func requestAuthToken(token, user, pass string) (authToken string, expiresIn int, err error) {
	realm, options, err := parseBearerToken(token)
	if err != nil {
		return
	}

	client := &http.Client{}

	for attempt := 0; attempt <= rateLimitMaxRetries; attempt++ {
		req, reqErr := http.NewRequest("GET", realm, nil)
		if reqErr != nil {
			return "", 0, reqErr
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

		resp, respErr := client.Do(req)
		if respErr != nil {
			err = fmt.Errorf("error in getting the token, http request failed %s", respErr)
			return
		}

		// Handle 429 rate limiting
		if handleRateLimitBackoff(resp, attempt, rateLimitMaxRetries) {
			resp.Body.Close()
			continue
		}

		if resp.StatusCode >= 400 {
			err = fmt.Errorf("authorization error %s", resp.Status)
			resp.Body.Close()
			return
		}

		var jsonResp map[string]interface{}
		err = json.NewDecoder(resp.Body).Decode(&jsonResp)
		if err != nil {
			resp.Body.Close()
			return
		}
		authTokenInterface, ok := jsonResp["token"]
		if ok {
			authToken = "Bearer " + authTokenInterface.(string)
		} else {
			err = fmt.Errorf("didn't get the token key from the server")
			resp.Body.Close()
			return
		}
		// Extract expires_in if present (typically 300s for Docker Hub)
		if expiresInInterface, ok := jsonResp["expires_in"]; ok {
			if v, ok := expiresInInterface.(float64); ok {
				expiresIn = int(v)
			}
		}
		resp.Body.Close()
		return
	}
	return "", 0, fmt.Errorf("max retries exceeded for rate limiting")
}

type LayerDownloader struct {
	image    *Image
	logger   *log.Entry
	token    string
	attempts map[string]int
	lock     sync.Mutex
}

func NewLayerDownloader(image *Image) LayerDownloader {
	return NewLayerDownloaderWithLogger(nil, image)
}

func NewLayerDownloaderWithLogger(logger *log.Entry, image *Image) LayerDownloader {
	return LayerDownloader{image: image, logger: l.Ensure(logger), token: "", attempts: make(map[string]int)}
}

func (ld *LayerDownloader) loggerForLayer(layer da.Layer) *log.Entry {
	return l.Ensure(ld.logger).WithField("layer", layer.Digest)
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
	logger := ld.loggerForLayer(layer)
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
		return ld.image.downloadLayerWithLogger(logger, layer, token)
	}
	inMem, err := ld.image.downloadLayerWithLogger(logger, layer, token)
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
	return ld.DownloadAndIngestWithLogger(nil, CVMFSRepo, layer)
}

func (ld *LayerDownloader) DownloadAndIngestWithLogger(logger *log.Entry, CVMFSRepo string, layer da.Layer) error {
	ld.logger = l.Ensure(logger)
	err := error(nil)
	for i := 0; i <= 5; i += 1 {
		to_ingest, err := ld.DownloadLayer(layer)
		if err != nil {
			// let's try again
			continue
		}
		defer to_ingest.Close()
		err = to_ingest.IngestIntoCVMFSWithLogger(ld.loggerForLayer(layer), CVMFSRepo)
		if err == nil {
			return nil
		}
	}
	return err
}

// CreateFlatOverlay uses cvmfs_server overlay to merge all image layers into a
// flat filesystem. It returns the singularity path (relative to /cvmfs/$REPO)
// where the merged image is placed.
// When ociConfigPath is non-empty, it is passed to the overlay command so that
// Singularity .singularity.d dotfiles are created atomically as part of the
// same transaction.
func (img *Image) CreateFlatOverlay(CVMFSRepo string, ociConfigPath string) (singularityPath string, err error) {
	return img.CreateFlatOverlayWithLogger(nil, CVMFSRepo, ociConfigPath, false)
}

func (img *Image) CreateFlatOverlayWithLogger(logger *log.Entry, CVMFSRepo string, ociConfigPath string, skipSingularity bool) (singularityPath string, err error) {
	logger = l.Ensure(logger)
	manifest, err := img.GetManifest()
	if err != nil {
		return
	}

	// Collect layer rootfs paths in bottom-to-top order (as they appear in the manifest)
	layerPaths := []string{}
	for _, layer := range manifest.Layers {
		if layer.MediaType == "application/vnd.docker.image.rootfs.foreign.diff.tar.gzip" {
			continue
		}
		layerDigest := strings.Split(layer.Digest, ":")[1]
		layerPaths = append(layerPaths, cvmfs.TrimCVMFSRepoPrefix(cvmfs.LayerRootfsPath(CVMFSRepo, layerDigest)))
	}

	if len(layerPaths) == 0 {
		err = fmt.Errorf("no layers found for image %s", img.GetSimpleName())
		return
	}

	singularityPath = manifest.GetSingularityPath()

	n := notification.NewNotification(NotificationService)
	n = n.AddField("image", img.GetSimpleName())

	t := time.Now()
	n.AddField("action", "start_overlay_merge").Send()

	err = cvmfs.OverlayWithLogger(logger, CVMFSRepo, layerPaths, singularityPath, ociConfigPath, skipSingularity)

	n.Elapsed(t).
		AddField("action", "end_overlay_merge").
		Error(err).
		Send()

	if err != nil {
		logger.WithField("error", err).Error("Error in creating flat overlay for image")
		return
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

// GetRawManifestBytes returns the exact wire bytes of the image manifest as
// served by the registry.  For multi-arch images the bytes belong to the
// resolved single-architecture manifest (matching what GetManifest returns).
// The bytes are cached after the first successful fetch so that repeated calls
// do not hit the network.
func (img *Image) GetRawManifestBytes() ([]byte, error) {
	if _, err := img.GetManifest(); err != nil {
		return nil, err
	}
	return img.rawManifestBytes, nil
}

// GetRawConfigBytes downloads and returns the raw config blob JSON for the
// image (the object referenced by manifest.Config.Digest).
func (img *Image) GetRawConfigBytes() ([]byte, error) {
	manifest, err := img.GetManifest()
	if err != nil {
		return nil, err
	}
	configURL := fmt.Sprintf("%sblobs/%s", img.GetBaseUrl(), manifest.Config.Digest)
	return makeGetRequest(configURL, map[string]string{
		"Accept": "application/vnd.docker.container.image.v1+json, application/vnd.oci.image.config.v1+json",
	})
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

	for attempt := 0; attempt <= rateLimitMaxRetries; attempt++ {
		req, reqErr := http.NewRequest("GET", url, nil)
		if reqErr != nil {
			l.LogE(reqErr).Error("Impossible to create a HTTP request")
			return nil, reqErr
		}

		req.Header.Set("Authorization", token)
		for k, v := range headers {
			req.Header.Set(k, v)
		}

		// for debugging: log curl command corresponding to request
		if logCurlEnabled() {
			curlcmd, curlErr := curling.NewFromRequest(req)
			if curlErr != nil {
				log.Fatal(curlErr)
			}
			log.Debug(curlcmd)
		}

		resp, respErr := client.Do(req)
		if respErr != nil {
			l.LogE(respErr).Error("Error in making the HTTP request")
			return nil, respErr
		}

		// Handle 429 rate limiting
		if handleRateLimitBackoff(resp, attempt, rateLimitMaxRetries) {
			resp.Body.Close()
			continue
		}

		body, bodyErr := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if bodyErr != nil {
			l.LogE(bodyErr).Error("Error in reading the second http response")
			return nil, bodyErr
		}

		return body, nil
	}

	return nil, fmt.Errorf("max retries exceeded for rate limiting")
}
