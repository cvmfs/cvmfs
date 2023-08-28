package registry

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cvmfs/ducc/config"
	"github.com/opencontainers/go-digest"
)

var localRegistriesMutex sync.Mutex
var localRegistries = make(map[ContainerRegistryIdentifier]*ContainerRegistry)
var registriesCtx context.Context

var defaultRateLimitIntervals = []RateLimit{{Interval: 1 * time.Minute, MaxCount: 200}, {Interval: 10 * time.Second, MaxCount: 50}}

const defaultExponentialBase = 2

type ContainerRegistryCredentials struct {
	Username string
	Password string
}

type ContainerRegistryIdentifier struct {
	Scheme   string
	Hostname string
	//port string TODO: Determine if this is needed
	//proxy string TODO: Determine if this is needed
}

type ContainerRegistry struct {
	Identifier ContainerRegistryIdentifier

	// Authentication
	Credentials ContainerRegistryCredentials
	tokenMutex  sync.Mutex
	token       string

	wwwAuthenticateMutex sync.Mutex
	wwwAuthenticate      string

	numAuthFailures         uint64
	backoffAndAuthTurnstile *BackoffTurnstile
	requestLimiter          *RequestLimiter

	Client *http.Client
}

func InitRegistries(ctx context.Context, registries []ContainerRegistryIdentifier, credentials map[ContainerRegistryIdentifier]ContainerRegistryCredentials) {
	registriesCtx = ctx
	for _, registry := range registries {
		// If the registry does not exist, create it
		GetOrCreateRegistry(registry)
	}
	for registry, credentials := range credentials {
		// Set the credentials for the registry
		GetOrCreateRegistry(registry).Credentials = credentials
	}
}

// GetOrCreateRegistry returns a pointer to a ContainerRegistry object.
// If the registry does not exist, it is created.
// If the registry does exist, a pointer to the existing registry is returned.
func GetOrCreateRegistry(identifier ContainerRegistryIdentifier) *ContainerRegistry {
	localRegistriesMutex.Lock()
	defer localRegistriesMutex.Unlock()

	if existingRegistry, ok := localRegistries[identifier]; ok {
		// Registry already exists
		return existingRegistry
	}

	// Registry does not exist, create it
	newRegistry := ContainerRegistry{
		Identifier: identifier,

		tokenMutex: sync.Mutex{},
		token:      "",

		backoffAndAuthTurnstile: NewBackoffTurnstile(registriesCtx, defaultExponentialBase, config.REGISTRY_INITIAL_BACKOFF, config.REGISTRY_MAX_BACKOFF),
		requestLimiter:          NewRequestLimiter(registriesCtx, config.REGISTRY_MAX_CONCURRENT_REQUESTS, defaultRateLimitIntervals),

		Client: &http.Client{},
	}

	return &newRegistry
}

// BaseUrl returns the base url of the registry for v2 requests.
func (cr ContainerRegistry) BaseUrl() string {
	return fmt.Sprintf("%s://%s/v2", cr.Identifier.Scheme, cr.Identifier.Hostname)
}

// PerformRequest performs a request to the registry. It blocks until a response is received.
// If the request returns a 401, a new token is requested and the request is retried.
// If the request returns a 429, the request is retried after a backoff.
// In both these cases, the function blocks until the response for the next request has been received.
func (cr *ContainerRegistry) PerformRequest(req *http.Request) (*http.Response, error) {
	var turnstileOnce sync.Once
	var tokenOnce sync.Once

	cr.requestLimiter.Enter()
	defer cr.requestLimiter.Exit()
retryRequest:
	// Don't send out new requests if the context is cancelled
	select {
	case <-registriesCtx.Done():
		return nil, fmt.Errorf("context cancelled")
	default:
	}

	cr.backoffAndAuthTurnstile.Enter()
	defer func() { turnstileOnce.Do(cr.backoffAndAuthTurnstile.Exit) }()
	cr.tokenMutex.Lock()
	defer func() { tokenOnce.Do(cr.tokenMutex.Unlock) }()

	if numAuthFailures := atomic.LoadUint64(&cr.numAuthFailures); numAuthFailures == 1 {
		// There has been an authentication failure, we need to get a new token before performing the request
		cr.wwwAuthenticateMutex.Lock()
		wwwAuthenticate := cr.wwwAuthenticate
		cr.wwwAuthenticateMutex.Unlock()
		var err error
		cr.token, err = requestAuthToken(wwwAuthenticate, cr.Credentials.Username, cr.Credentials.Password)
		if err != nil {
			if cr.Credentials.Username == "" && cr.Credentials.Password == "" {
				// We already tried with no username and password, return the error
				atomic.AddUint64(&cr.numAuthFailures, 1)
				return nil, err
			}
			// As a last resort, try again with no username and password
			cr.token, err = requestAuthToken(wwwAuthenticate, "", "")
			if err != nil {
				atomic.AddUint64(&cr.numAuthFailures, 1)
				return nil, err
			}
		}
		atomic.StoreUint64(&cr.numAuthFailures, 0)
	} else if numAuthFailures > 1 {
		// We already tried to authenticate, but it failed
		return nil, fmt.Errorf("authentication failed")
	}
	// If we have a token, add it to the request
	if cr.token != "" {
		req.Header.Set("Authorization", cr.token)
	}
	tokenOnce.Do(cr.tokenMutex.Unlock)
	turnstileOnce.Do(cr.backoffAndAuthTurnstile.Exit)

	// Perform the request
	res, err := cr.Client.Do(req)
	if err != nil {
		// Error performing request
		return nil, err
	}

	// We got a good response, everything is fine
	if res.StatusCode < 300 && res.StatusCode >= 200 {
		// Valid response, reset the exponential backoff
		cr.backoffAndAuthTurnstile.ResetExponentialBackoff()
		return res, nil
	}

	if res.StatusCode == http.StatusTooManyRequests {
		// We are rate limited, handle the backoff and retry
		res.Body.Close()
		cr.backoffAndAuthTurnstile.SetBackoff(res)
		goto retryRequest
	}

	if res.StatusCode == http.StatusUnauthorized {
		// We are unauthorized
		cr.wwwAuthenticateMutex.Lock()
		cr.wwwAuthenticate = res.Header["Www-Authenticate"][0]
		cr.wwwAuthenticateMutex.Unlock()
		res.Body.Close()
		// If we are not currently in an authentication failure, set the counter to 1
		atomic.CompareAndSwapUint64(&cr.numAuthFailures, 0, 1)
		goto retryRequest
	}
	return res, err
}

// TODO: This is taken directly from the previous code, must be checked
func requestAuthToken(token, user, pass string) (authToken string, err error) {
	realm, options, err := parseBearerToken(token)
	if err != nil {
		return
	}
	req, err := http.NewRequestWithContext(registriesCtx, "GET", realm, nil)
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

// TODO: This is taken directly from the previous code, must be checked
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

func (cr *ContainerRegistry) DownloadBlob(blobDigest digest.Digest, repository string, acceptHeaders []string) error {
	url := fmt.Sprintf("%s/%s/blobs/%s", cr.BaseUrl(), repository, blobDigest.String())
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return fmt.Errorf("error in creating request: %s", err)
	}

	for _, header := range acceptHeaders {
		req.Header.Add("Accept", header)
	}

	res, err := cr.PerformRequest(req)
	if err != nil {
		return fmt.Errorf("error in fetching blob: %s", err)
	}
	defer res.Body.Close()

	if 200 > res.StatusCode || res.StatusCode >= 300 {
		return fmt.Errorf("error in fetching blob: %s", res.Status)
	}

	os.MkdirAll(path.Join(config.TMP_FILE_PATH, "blobs"), os.FileMode(0755))
	filePath := path.Join(config.TMP_FILE_PATH, "blobs", blobDigest.Encoded())
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(0644))
	if err != nil {
		return fmt.Errorf("error in creating file: %s", err)
	}
	// Verify the checksum while writing to the file
	reader := bufio.NewReader(io.TeeReader(res.Body, file))
	var checksum digest.Digest
	var checksumErr error
	checksum, err = digest.SHA256.FromReader(reader)
	file.Close()
	if err != nil {
		err = fmt.Errorf("error in calculating checksum: %s", checksumErr)
	}
	if checksum != blobDigest {
		err = fmt.Errorf("checksum mismatch: expected %s, got %s", blobDigest.String(), checksum.String())
	}
	if err != nil {
		// Something went wrong, remove the file
		fmt.Printf("Error: %s\n", err.Error())
		os.Remove(filePath)
		return err
	}
	return nil
}
