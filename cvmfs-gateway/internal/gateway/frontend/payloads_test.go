package frontend

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/julienschmidt/httprouter"
)

func TestPayloadHandlerLegacy(t *testing.T) {
	backend := mockBackend{}
	token := "lease_token"

	msg, _ := json.Marshal(map[string]interface{}{
		"session_token":  token,
		"payload_digest": "abcdef",
		"header_size":    "123",
		"api_version":    "2",
	})

	req := httptest.NewRequest("POST", "/api/v1/payloads", bytes.NewReader(msg))
	HMAC := ComputeHMAC(msg, backend.GetKey(context.TODO(), "keyid2").Secret)
	req.Header["Authorization"] = []string{"keyid2 " + base64.StdEncoding.EncodeToString(HMAC)}
	req.Header["Message-Size"] = []string{strconv.Itoa(len(msg))}

	w := httptest.NewRecorder()
	handler := MakePayloadsHandler(&backend)

	ps := httprouter.Params{}
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

func TestPayloadHandlerNew(t *testing.T) {
	backend := mockBackend{}
	token := "lease_token"

	msg, _ := json.Marshal(map[string]interface{}{
		"payload_digest": "abcdef",
		"header_size":    "123",
		"api_version":    "3",
	})

	req := httptest.NewRequest("POST", "/api/v1/payloads", bytes.NewReader(msg))
	HMAC := ComputeHMAC([]byte(token), backend.GetKey(context.TODO(), "keyid2").Secret)
	req.Header["Authorization"] = []string{"keyid2 " + base64.StdEncoding.EncodeToString(HMAC)}
	req.Header["Message-Size"] = []string{fmt.Sprintf("%v", len(msg))}

	w := httptest.NewRecorder()
	handler := MakePayloadsHandler(&backend)

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
