package registry

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
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
type tokenAuth struct {
	token           string
	wwwAuthenticate string
	mutex           sync.Mutex
	numAuthFailures uint64
}

type ContainerRegistry struct {
	Identifier ContainerRegistryIdentifier

	// Authentication
	Credentials ContainerRegistryCredentials
	authMutex   sync.Mutex
	tokens      map[string]*tokenAuth

	numAuthFailures uint64
	backoffGuard    *BackoffGuard
	requestLimiter  *RequestLimiter

	Client *http.Client
}

func InitRegistriesFromEnv(ctx context.Context) {
	regs := os.Getenv("DUCC_AUTH_REGISTRIES")

	identifiers := make([]ContainerRegistryIdentifier, 0)
	credentials := make(map[ContainerRegistryIdentifier]ContainerRegistryCredentials)
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

		newIdentifier := ContainerRegistryIdentifier{
			Scheme:   "https",
			Hostname: ident,
		}

		identifiers = append(identifiers, newIdentifier)
		credentials[newIdentifier] = ContainerRegistryCredentials{
			Username: user,
			Password: pass,
		}
	}
	InitRegistries(ctx, identifiers, credentials)
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

		authMutex: sync.Mutex{},
		tokens:    make(map[string]*tokenAuth),

		backoffGuard:   NewBackoffGuard(registriesCtx, defaultExponentialBase, config.REGISTRY_INITIAL_BACKOFF, config.REGISTRY_MAX_BACKOFF),
		requestLimiter: NewRequestLimiter(registriesCtx, config.REGISTRY_MAX_CONCURRENT_REQUESTS, defaultRateLimitIntervals),

		Client: &http.Client{},
	}

	localRegistries[identifier] = &newRegistry
	return &newRegistry
}

// BaseUrl returns the base url of the registry for v2 requests.
func (cr *ContainerRegistry) BaseUrl() string {
	return fmt.Sprintf("%s://%s/v2", cr.Identifier.Scheme, cr.Identifier.Hostname)
}

// PerformRequest performs a request to the registry. It blocks until a response is received.
// If the request returns a 401, a new token is requested and the request is retried.
// If the request returns a 429, the request is retried after a backoff.
// In both these cases, the function blocks until the response for the next request has been received.
func (cr *ContainerRegistry) PerformRequest(req *http.Request, repository string) (*http.Response, error) {
	cr.requestLimiter.Enter()
	defer cr.requestLimiter.Exit()
	var auth *tokenAuth

retryRequest:
	// Don't send out new requests if the context is cancelled
	select {
	case <-registriesCtx.Done():
		return nil, fmt.Errorf("context cancelled")
	default:
	}

	cr.authMutex.Lock()

	// We use per-repository tokens, as the scope of the token is usually the repository
	if repository != "" {
		var ok bool
		auth, ok = cr.tokens[repository]
		if !ok {
			auth = &tokenAuth{}
			cr.tokens[repository] = auth
		}
	} else {
		auth = &tokenAuth{}
	}
	auth.mutex.Lock() // Lock this repository
	once := sync.Once{}
	defer once.Do(auth.mutex.Unlock)
	cr.authMutex.Unlock() // Unlock the registry-wide map

	// Wait for potential backoff to finish
	cr.backoffGuard.Enter()

	if numAuthFailures := atomic.LoadUint64(&auth.numAuthFailures); numAuthFailures >= 1 {
		// There has been an authentication failure, we need to get a new token before performing the request
		var err error
		auth.token, err = requestAuthToken(auth.wwwAuthenticate, cr.Credentials.Username, cr.Credentials.Password)
		if err != nil {
			if cr.Credentials.Username == "" && cr.Credentials.Password == "" {
				// We already tried with no username and password, return the error
				atomic.AddUint64(&cr.numAuthFailures, 1)
				return nil, err
			}
			// As a last resort, try again with no username and password
			auth.token, err = requestAuthToken(auth.wwwAuthenticate, "", "")
			if err != nil {
				atomic.AddUint64(&cr.numAuthFailures, 1)
				return nil, err
			}
		}
		atomic.StoreUint64(&cr.numAuthFailures, 0)
	} else if numAuthFailures > 3 {
		// We already tried to authenticate twice, but it failed
		return nil, fmt.Errorf("authentication failed")
	}
	// If we have a token, add it to the request
	if auth.token != "" {
		req.Header.Set("Authorization", auth.token)
	}
	once.Do(auth.mutex.Unlock)

	// Perform the request
	res, err := cr.Client.Do(req)
	if err != nil {
		// Error performing request
		return nil, err
	}

	// We got a good response, everything is fine
	if res.StatusCode < 300 && res.StatusCode >= 200 {
		// Valid response, reset the exponential backoff
		cr.backoffGuard.ResetExponentialBackoff()
		return res, nil
	}

	if res.StatusCode == http.StatusTooManyRequests {
		// We are rate limited, handle the backoff and retry
		res.Body.Close()
		cr.backoffGuard.SetBackoff(res)
		goto retryRequest
	}

	if res.StatusCode == http.StatusUnauthorized {
		// We are unauthorized, store auth info
		auth.mutex.Lock()
		auth.wwwAuthenticate = res.Header["Www-Authenticate"][0]
		auth.mutex.Unlock()
		res.Body.Close()
		// If we are not already in an authentication failure, set the counter to 1
		atomic.CompareAndSwapUint64(&auth.numAuthFailures, 0, 1)
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

	res, err := cr.PerformRequest(req, repository)
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
