package frontend

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
)

func TestAuthorizationMiddlewareGET(t *testing.T) {
	backend := mockBackend{}
	t.Run("GET requires no authorization", func(t *testing.T) {
		reqBody := []byte("hello")
		req := httptest.NewRequest("GET", "/api/v1/repos", bytes.NewReader(reqBody))
		w := httptest.NewRecorder()
		handler := MakeAuthzMiddleware(&backend)(http.HandlerFunc(forwardBody))

		handler.ServeHTTP(w, req)

		resp := w.Result()

		if resp.StatusCode != 200 {
			t.Errorf("Invalid HTTP response status code: %v", resp.StatusCode)
		}

		respBody, _ := ioutil.ReadAll(resp.Body)
		if !bytes.Equal(reqBody, respBody) {
			t.Errorf("Invalid response body: %v", respBody)
		}
	})
}
func TestAuthorizationMiddlewareNewLease(t *testing.T) {
	backend := mockBackend{}
	t.Run("POST /leases (new lease OK)", func(t *testing.T) {
		reqBody := []byte("hello")
		HMAC := ComputeHMAC(reqBody, backend.GetSecret("keyid2"))
		req := httptest.NewRequest("POST", "/api/v1/leases", bytes.NewReader(reqBody))
		req.Header["Authorization"] = []string{"keyid2 " + base64.StdEncoding.EncodeToString(HMAC)}
		w := httptest.NewRecorder()
		handler := MakeAuthzMiddleware(&backend)(http.HandlerFunc(forwardBody))

		handler.ServeHTTP(w, req)

		resp := w.Result()

		respBody, _ := ioutil.ReadAll(resp.Body)
		if !bytes.Equal([]byte(reqBody), respBody) {
			t.Errorf("Invalid response body: %v", string(respBody))
		}
	})
	t.Run("POST /leases (new lease missing auth header)", func(t *testing.T) {
		reqBody := []byte("hello")
		req := httptest.NewRequest("POST", "/api/v1/leases", bytes.NewReader(reqBody))
		w := httptest.NewRecorder()
		handler := MakeAuthzMiddleware(&backend)(http.HandlerFunc(forwardBody))

		handler.ServeHTTP(w, req)

		resp := w.Result()

		respBody, _ := ioutil.ReadAll(resp.Body)
		if !bytes.Equal([]byte("{\"reason\":\"invalid_hmac\",\"status\":\"error\"}"), respBody) {
			t.Errorf("Invalid response body: %v", string(respBody))
		}
	})
	t.Run("POST /leases (new lease invalid auth header)", func(t *testing.T) {
		reqBody := []byte("hello")
		req := httptest.NewRequest("POST", "/api/v1/leases", bytes.NewReader(reqBody))
		w := httptest.NewRecorder()
		req.Header["Authorization"] = []string{"rubbish"}
		handler := MakeAuthzMiddleware(&backend)(http.HandlerFunc(forwardBody))

		handler.ServeHTTP(w, req)

		resp := w.Result()

		respBody, _ := ioutil.ReadAll(resp.Body)
		if !bytes.Equal([]byte("{\"reason\":\"invalid_hmac\",\"status\":\"error\"}"), respBody) {
			t.Errorf("Invalid response body: %v", string(respBody))
		}
	})
	t.Run("POST /leases (new lease invalid HMAC)", func(t *testing.T) {
		reqBody := []byte("hello")
		HMAC := ComputeHMAC([]byte("other HMAC input"), backend.GetSecret("keyid2"))
		req := httptest.NewRequest("POST", "/api/v1/leases", bytes.NewReader(reqBody))
		w := httptest.NewRecorder()
		req.Header["Authorization"] = []string{"keyid2 " + base64.StdEncoding.EncodeToString(HMAC)}
		handler := MakeAuthzMiddleware(&backend)(http.HandlerFunc(forwardBody))

		handler.ServeHTTP(w, req)

		resp := w.Result()

		respBody, _ := ioutil.ReadAll(resp.Body)
		if !bytes.Equal([]byte("{\"reason\":\"invalid_hmac\",\"status\":\"error\"}"), respBody) {
			t.Errorf("Invalid response body: %v", string(respBody))
		}
	})
}

func TestAuthorizationMiddlewareCommitLease(t *testing.T) {
	backend := mockBackend{}
	token := "lease_token"
	t.Run("POST /leases/$token (commit lease OK)", func(t *testing.T) {
		HMAC := ComputeHMAC([]byte(token), backend.GetSecret("keyid2"))
		req := httptest.NewRequest("POST", "/api/v1/leases/"+token, nil)
		req.Header["Authorization"] = []string{"keyid2 " + base64.StdEncoding.EncodeToString(HMAC)}
		w := httptest.NewRecorder()
		handler := MakeAuthzMiddleware(&backend)(http.HandlerFunc(forwardBody))

		handler.ServeHTTP(w, req)

		resp := w.Result()

		if resp.StatusCode != 200 {
			t.Errorf("Invalid HTTP response status code: %v", resp.StatusCode)
		}
	})
	t.Run("POST /leases (commit lease invalid HMAC)", func(t *testing.T) {
		reqBody := []byte("hello")
		HMAC := ComputeHMAC([]byte("other HMAC input"), backend.GetSecret("keyid2"))
		req := httptest.NewRequest("POST", "/api/v1/leases/"+token, bytes.NewReader(reqBody))
		w := httptest.NewRecorder()
		req.Header["Authorization"] = []string{"keyid2 " + base64.StdEncoding.EncodeToString(HMAC)}
		handler := MakeAuthzMiddleware(&backend)(http.HandlerFunc(forwardBody))

		handler.ServeHTTP(w, req)

		resp := w.Result()

		respBody, _ := ioutil.ReadAll(resp.Body)
		if !bytes.Equal([]byte("{\"reason\":\"invalid_hmac\",\"status\":\"error\"}"), respBody) {
			t.Errorf("Invalid response body: %v", string(respBody))
		}
	})
}

func TestAuthorizationMiddlewareSubmitPayloadLegacy(t *testing.T) {
	backend := mockBackend{}
	token := "lease_token"

	msg, _ := json.Marshal(map[string]string{
		"session_token":  token,
		"payload_digest": "abcdef",
		"header_size":    "123",
		"api_version":    "2",
	})

	HMAC := ComputeHMAC(msg, backend.GetSecret("keyid2"))
	req := httptest.NewRequest("POST", "/api/v1/payloads", bytes.NewReader(msg))
	req.Header["Authorization"] = []string{"keyid2 " + base64.StdEncoding.EncodeToString(HMAC)}
	req.Header["Message-Size"] = []string{fmt.Sprintf("%v", len(msg))}
	w := httptest.NewRecorder()
	handler := MakeAuthzMiddleware(&backend)(http.HandlerFunc(forwardBody))

	handler.ServeHTTP(w, req)

	resp := w.Result()

	if resp.StatusCode != 200 {
		t.Errorf("Invalid HTTP response status code: %v", resp.StatusCode)
	}

	respBody, _ := ioutil.ReadAll(resp.Body)
	if !bytes.Equal(msg, respBody) {
		t.Errorf("Invalid response body: %v", string(respBody))
	}
}

func TestAuthorizationMiddlewareSubmitPayloadNew(t *testing.T) {
	backend := mockBackend{}
	token := "lease_token"

	msg, _ := json.Marshal(map[string]string{
		"payload_digest": "abcdef",
		"header_size":    "123",
		"api_version":    "2",
	})

	HMAC := ComputeHMAC([]byte(token), backend.GetSecret("keyid2"))
	req := httptest.NewRequest("POST", "/api/v1/payloads/"+token, bytes.NewReader(msg))

	req = mux.SetURLVars(req, map[string]string{"token": token})

	req.Header["Authorization"] = []string{"keyid2 " + base64.StdEncoding.EncodeToString(HMAC)}
	req.Header["Message-Size"] = []string{fmt.Sprintf("%v", len(msg))}
	w := httptest.NewRecorder()
	handler := MakeAuthzMiddleware(&backend)(http.HandlerFunc(forwardBody))

	handler.ServeHTTP(w, req)

	resp := w.Result()

	if resp.StatusCode != 200 {
		t.Errorf("Invalid HTTP response status code: %v", resp.StatusCode)
	}

	respBody, _ := ioutil.ReadAll(resp.Body)
	if !bytes.Equal(msg, respBody) {
		t.Errorf("Invalid response body: %v", string(respBody))
	}
}
