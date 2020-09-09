package frontend

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io/ioutil"
	"net/http/httptest"
	"testing"

	"github.com/julienschmidt/httprouter"
)

func TestLeaseHandlerNewLease(t *testing.T) {
	backend := mockBackend{}
	msg, _ := json.Marshal(map[string]interface{}{
		"path":        "test2.repo.org/some/path",
		"api_version": "3",
	})

	req := httptest.NewRequest("POST", "/api/v1/leases", bytes.NewReader(msg))
	HMAC := ComputeHMAC(msg, backend.GetKey(context.TODO(), "keyid2").Secret)
	req.Header["Authorization"] = []string{"keyid2 " + base64.StdEncoding.EncodeToString(HMAC)}

	w := httptest.NewRecorder()
	handler := MakeLeasesHandler(&backend)

	ps := httprouter.Params{}
	handler(w, req, ps)

	expected, _ := json.Marshal(map[string]interface{}{
		"status":          "ok",
		"session_token":   "lease_token_string",
		"max_api_version": 3,
	})

	resp := w.Result()

	if resp.StatusCode != 200 {
		t.Errorf("Invalid HTTP response status code: %v", resp.StatusCode)
	}

	respBody, _ := ioutil.ReadAll(resp.Body)
	if !bytes.Equal(respBody, expected) {
		t.Errorf("Invalid response body: %v", string(respBody))
	}
}

func TestLeaseHandlerCancelLease(t *testing.T) {
	backend := mockBackend{}
	token := "lease_token"
	req := httptest.NewRequest("DELETE", "/api/v1/leases/"+token, nil)
	HMAC := ComputeHMAC([]byte(token), backend.GetKey(context.TODO(), "keyid2").Secret)
	req.Header["Authorization"] = []string{"keyid2 " + base64.StdEncoding.EncodeToString(HMAC)}

	w := httptest.NewRecorder()
	handler := MakeLeasesHandler(&backend)

	ps := httprouter.Params{httprouter.Param{Key: "token", Value: token}}
	handler(w, req, ps)

	expected, _ := json.Marshal(map[string]interface{}{
		"status": "ok",
	})

	resp := w.Result()

	if resp.StatusCode != 200 {
		t.Errorf("Invalid HTTP response status code: %v", resp.StatusCode)
	}

	respBody, _ := ioutil.ReadAll(resp.Body)
	if !bytes.Equal(respBody, expected) {
		t.Errorf("Invalid response body: %v", string(respBody))
	}
}

func TestLeaseHandlerCommitLease(t *testing.T) {
	backend := mockBackend{}
	token := "lease_token"

	msg, _ := json.Marshal(map[string]interface{}{
		"old_root_hash":   "abcdef",
		"new_root_hash":   "defabc",
		"tag_name":        "tag1",
		"tag_channel":     "main",
		"tag_description": "this is a tag",
	})

	req := httptest.NewRequest("POST", "/api/v1/leases/"+token, bytes.NewReader(msg))
	HMAC := ComputeHMAC([]byte(token), backend.GetKey(context.TODO(), "keyid2").Secret)
	req.Header["Authorization"] = []string{"keyid2 " + base64.StdEncoding.EncodeToString(HMAC)}

	w := httptest.NewRecorder()
	handler := MakeLeasesHandler(&backend)

	ps := httprouter.Params{httprouter.Param{Key: "token", Value: token}}
	handler(w, req, ps)

	expected, _ := json.Marshal(map[string]interface{}{
		"status":         "ok",
		"final_revision": 1,
	})

	resp := w.Result()

	if resp.StatusCode != 200 {
		t.Errorf("Invalid HTTP response status code: %v", resp.StatusCode)
	}

	respBody, _ := ioutil.ReadAll(resp.Body)
	if !bytes.Equal(respBody, expected) {
		t.Errorf("Invalid response body: %v", string(respBody))
	}
}
