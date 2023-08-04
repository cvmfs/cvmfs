package registry

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
)

var localRegistriesMutex sync.Mutex
var localRegistries = make(map[ContainerRegistryIdentifier]*ContainerRegistry)

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
	Credentials     ContainerRegistryCredentials
	TokenCv         *sync.Cond
	token           string
	gotToken        bool
	waitingForToken bool
	// TODO: Some kind of auth error variable that can be checked
	// Number of simultaneous connections to the registry

	Client *http.Client
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
		TokenCv:    sync.NewCond(&sync.Mutex{}),
		Client:     &http.Client{},
	}

	return &newRegistry
}

// BaseUrl returns the base url of the registry for v2 requests.
func (cr ContainerRegistry) BaseUrl() string {
	return fmt.Sprintf("%s://%s/v2", cr.Identifier.Scheme, cr.Identifier.Hostname)
}

// waitUntilReadyToPerformRequest blocks until the registry is ready to perform a request.
func (cr *ContainerRegistry) waitUntilReadyToPerformRequest() {
	// If we are waiting for a new token, hold the request until we get one
	// TODO: Backoff after 429 Too Many Requests
	cr.TokenCv.L.Lock()
	for cr.waitingForToken {
		cr.TokenCv.Wait()
	}
	cr.TokenCv.L.Unlock()
}

// PerformRequest performs a request to the registry. It blocks until a response is received.
// If the request returns a 401, a new token is requested and the request is retried.
// If the request returns a 429, the request is retried after a backoff.
// In both these cases, the function blocks until the response for the next request has been received.
func (cr *ContainerRegistry) PerformRequest(req *http.Request) (*http.Response, error) {
retryRequest:
	cr.waitUntilReadyToPerformRequest()

	// If we have a token, add it to the request
	cr.TokenCv.L.Lock()
	tokenToSend := cr.token
	if cr.gotToken {
		req.Header.Set("Authorization", tokenToSend)
	}
	cr.TokenCv.L.Unlock()

	// Perform the request
	res, err := cr.Client.Do(req)
	if err != nil {
		// Error performing request
		return nil, err
	}

	// We got a good response, everything is fine
	if res.StatusCode < 300 && res.StatusCode >= 200 {
		// TODO: logging
		return res, nil
	}

	// We are rate limited
	if res.StatusCode == http.StatusTooManyRequests {
		res.Body.Close()
		// TODO: Handle rate limit wait
		goto retryRequest
	}
	if res.StatusCode == http.StatusUnauthorized {
		WwwAuthenticate := res.Header["Www-Authenticate"][0]
		res.Body.Close()

		cr.TokenCv.L.Lock()
		if cr.waitingForToken || tokenToSend != cr.token {
			// Another thread has already requested a new token
			cr.TokenCv.L.Unlock()
			goto retryRequest
		}

		// We need to request a new token
		cr.waitingForToken = true
		cr.TokenCv.L.Unlock()

		token, err := requestAuthToken(WwwAuthenticate, cr.Credentials.Username, cr.Credentials.Password)
		if err != nil {
			// As a last resort, try again with no username and password
			if cr.Credentials.Username == "" && cr.Credentials.Password == "" {
				// We already tried with no username and password, return the error
				return nil, err
			}
			token, err = requestAuthToken(WwwAuthenticate, "", "")
			if err != nil {
				return nil, err
			}
		}
		cr.TokenCv.L.Lock()
		cr.token = token
		cr.gotToken = true
		cr.waitingForToken = false
		cr.TokenCv.L.Unlock()
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
