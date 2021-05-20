package frontend

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/julienschmidt/httprouter"
)

func TestAuthorizationMiddlewareNewLease(t *testing.T) {
	backend := mockBackend{}
	t.Run("POST /leases (new lease OK)", func(t *testing.T) {
		reqBody := []byte("hello")
		HMAC := ComputeHMAC(reqBody, backend.GetKey(context.TODO(), "keyid2").Secret)
		req := httptest.NewRequest("POST", "/api/v1/leases", bytes.NewReader(reqBody))
		req.Header["Authorization"] = []string{"keyid2 " + base64.StdEncoding.EncodeToString(HMAC)}
		w := httptest.NewRecorder()
		handler := WithAuthz(&backend, forwardBody)

		ps := httprouter.Params{}
		handler(w, req, ps)

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
		handler := WithAuthz(&backend, forwardBody)

		ps := httprouter.Params{}
		handler(w, req, ps)

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
		handler := WithAuthz(&backend, forwardBody)

		ps := httprouter.Params{}
		handler(w, req, ps)

		resp := w.Result()

		respBody, _ := ioutil.ReadAll(resp.Body)
		if !bytes.Equal([]byte("{\"reason\":\"invalid_hmac\",\"status\":\"error\"}"), respBody) {
			t.Errorf("Invalid response body: %v", string(respBody))
		}
	})
	t.Run("POST /leases (new lease invalid HMAC)", func(t *testing.T) {
		reqBody := []byte("hello")
		HMAC := ComputeHMAC([]byte("other HMAC input"), backend.GetKey(context.TODO(), "keyid2").Secret)
		req := httptest.NewRequest("POST", "/api/v1/leases", bytes.NewReader(reqBody))
		w := httptest.NewRecorder()
		req.Header["Authorization"] = []string{"keyid2 " + base64.StdEncoding.EncodeToString(HMAC)}
		handler := WithAuthz(&backend, forwardBody)

		ps := httprouter.Params{}
		handler(w, req, ps)

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
		HMAC := ComputeHMAC([]byte(token), backend.GetKey(context.TODO(), "keyid2").Secret)
		req := httptest.NewRequest("POST", "/api/v1/leases/"+token, nil)
		req.Header["Authorization"] = []string{"keyid2 " + base64.StdEncoding.EncodeToString(HMAC)}
		w := httptest.NewRecorder()
		handler := WithAuthz(&backend, forwardBody)

		ps := httprouter.Params{}
		handler(w, req, ps)

		resp := w.Result()

		if resp.StatusCode != 200 {
			t.Errorf("Invalid HTTP response status code: %v", resp.StatusCode)
		}
	})
	t.Run("POST /leases (commit lease invalid HMAC)", func(t *testing.T) {
		reqBody := []byte("hello")
		HMAC := ComputeHMAC([]byte("other HMAC input"), backend.GetKey(context.TODO(), "keyid2").Secret)
		req := httptest.NewRequest("POST", "/api/v1/leases/"+token, bytes.NewReader(reqBody))
		w := httptest.NewRecorder()
		req.Header["Authorization"] = []string{"keyid2 " + base64.StdEncoding.EncodeToString(HMAC)}
		handler := WithAuthz(&backend, forwardBody)

		ps := httprouter.Params{}
		handler(w, req, ps)

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

	HMAC := ComputeHMAC(msg, backend.GetKey(context.TODO(), "keyid2").Secret)
	req := httptest.NewRequest("POST", "/api/v1/payloads", bytes.NewReader(msg))
	req.Header["Authorization"] = []string{"keyid2 " + base64.StdEncoding.EncodeToString(HMAC)}
	req.Header["Message-Size"] = []string{strconv.Itoa(len(msg))}
	w := httptest.NewRecorder()
	handler := WithAuthz(&backend, forwardBody)

	ps := httprouter.Params{}
	handler(w, req, ps)

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
		"api_version":    "3",
	})

	HMAC := ComputeHMAC([]byte(token), backend.GetKey(context.TODO(), "keyid2").Secret)
	req := httptest.NewRequest("POST", "/api/v1/payloads/"+token, bytes.NewReader(msg))

	ps := httprouter.Params{httprouter.Param{Key: "token", Value: token}}

	req.Header["Authorization"] = []string{"keyid2 " + base64.StdEncoding.EncodeToString(HMAC)}
	req.Header["Message-Size"] = []string{strconv.Itoa(len(msg))}
	w := httptest.NewRecorder()
	handler := WithAuthz(&backend, forwardBody)

	handler(w, req, ps)

	resp := w.Result()

	if resp.StatusCode != 200 {
		t.Errorf("Invalid HTTP response status code: %v", resp.StatusCode)
	}

	respBody, _ := ioutil.ReadAll(resp.Body)
	if !bytes.Equal(msg, respBody) {
		t.Errorf("Invalid response body: %v", string(respBody))
	}
}

func TestAdminAuthorizationMiddleware(t *testing.T) {
	backend := mockBackend{}

	t.Run("Non-admin key is rejected", func(t *testing.T) {
		HMAC := ComputeHMAC([]byte("/api/v1/repos/test1.repo.org"), backend.GetKey(context.TODO(), "keyid2").Secret)
		req := httptest.NewRequest("DELETE", "/api/v1/repos/test1.repo.org", nil)
		ps := httprouter.Params{}

		req.Header["Authorization"] = []string{"keyid2 " + base64.StdEncoding.EncodeToString(HMAC)}
		w := httptest.NewRecorder()
		handler := WithAdminAuthz(&backend, forwardBody)

		handler(w, req, ps)

		resp := w.Result()

		if resp.StatusCode != 200 {
			t.Errorf("Invalid HTTP response status code: %v", resp.StatusCode)
		}

		respBody, _ := ioutil.ReadAll(resp.Body)
		if !bytes.Equal([]byte("{\"reason\":\"no_admin_key\",\"status\":\"error\"}"), respBody) {
			t.Errorf("Invalid response body: %v", string(respBody))
		}
	})
	t.Run("Invalid HTTP method", func(t *testing.T) {
		HMAC := ComputeHMAC([]byte("/api/v1/repos/test1.repo.org"), backend.GetKey(context.TODO(), "admin0").Secret)
		req := httptest.NewRequest("PUT", "/api/v1/repos/test1.repo.org", nil)
		ps := httprouter.Params{}

		req.Header["Authorization"] = []string{"admin0 " + base64.StdEncoding.EncodeToString(HMAC)}
		w := httptest.NewRecorder()
		handler := WithAdminAuthz(&backend, forwardBody)

		handler(w, req, ps)

		resp := w.Result()

		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("Invalid HTTP response status code: %v", resp.StatusCode)
		}
	})
	t.Run("DELETE", func(t *testing.T) {
		msg := []byte("hello")

		HMAC := ComputeHMAC([]byte("/api/v1/repos/test1.repo.org"), backend.GetKey(context.TODO(), "admin0").Secret)
		req := httptest.NewRequest("DELETE", "/api/v1/repos/test1.repo.org", bytes.NewReader(msg))
		ps := httprouter.Params{}

		req.Header["Authorization"] = []string{"admin0 " + base64.StdEncoding.EncodeToString(HMAC)}
		w := httptest.NewRecorder()
		handler := WithAdminAuthz(&backend, forwardBody)

		handler(w, req, ps)

		resp := w.Result()

		if resp.StatusCode != 200 {
			t.Errorf("Invalid HTTP response status code: %v", resp.StatusCode)
		}

		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Errorf("Could not read response body")
		}
		if !bytes.Equal(msg, respBody) {
			t.Errorf("Invalid response body: %v", string(respBody))
		}
	})
	t.Run("POST", func(t *testing.T) {
		msg := []byte("hello")

		HMAC := ComputeHMAC(msg, backend.GetKey(context.TODO(), "admin0").Secret)
		req := httptest.NewRequest("POST", "/api/v1/repos/test1.repo.org", bytes.NewReader(msg))
		ps := httprouter.Params{}

		req.Header["Authorization"] = []string{"admin0 " + base64.StdEncoding.EncodeToString(HMAC)}
		w := httptest.NewRecorder()
		handler := WithAdminAuthz(&backend, forwardBody)

		handler(w, req, ps)

		resp := w.Result()

		if resp.StatusCode != 200 {
			t.Errorf("Invalid HTTP response status code: %v", resp.StatusCode)
		}

		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Errorf("Could not read response body")
		}
		if !bytes.Equal(msg, respBody) {
			t.Errorf("Invalid response body: %v", string(respBody))
		}
	})
}
