package rest

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"

	"testing"

	"github.com/cvmfs/ducc/db"
	"github.com/cvmfs/ducc/test"
)

func TestGetAllWishes(t *testing.T) {
	db.Init(db.CreateInMemDBFromFixture("../db/testdata/fixture1.sql"))

	t.Run("Golden1", func(t *testing.T) {
		req, err := http.NewRequest("GET", "/wishes", nil)
		if err != nil {
			t.Fatal(err)
		}
		goldenTestHttpRequest(*req, "GetAllWishes1", http.StatusOK, t)
	})
}

func TestGetWishByID(t *testing.T) {
	db.Init(db.CreateInMemDBFromFixture("../db/testdata/fixture1.sql"))

	t.Run("Golden1", func(t *testing.T) {
		req, err := http.NewRequest("GET", "/wishes/3b971878-56d7-4cf3-8efc-cea93cdfdecc", nil)
		if err != nil {
			t.Fatal(err)
		}
		goldenTestHttpRequest(*req, "GetWishByID1", http.StatusOK, t)
	})

	t.Run("NonExistent", func(t *testing.T) {
		req, err := http.NewRequest("GET", "/wishes/00000000-0000-0000-0000-000000000000", nil)
		if err != nil {
			t.Fatal(err)
		}
		testHttpResponseCode(*req, http.StatusNotFound, t)
	})
}

func TestGetAllImages(t *testing.T) {
	db.Init(db.CreateInMemDBFromFixture("../db/testdata/fixture1.sql"))

	t.Run("Golden1", func(t *testing.T) {
		req, err := http.NewRequest("GET", "/images", nil)
		if err != nil {
			t.Fatal(err)
		}
		goldenTestHttpRequest(*req, "GetAllImages1", http.StatusOK, t)
	})
}

// goldenTestHttpRequest is a helper function for testing HTTP requests against golden files.
// It takes in a request, a name, a required status code and a testing object.
// It then compares the response to a golden file with the same name as the name parameter.
// If the test is run with the -update flag, (*test.UpdateTests is true)the golden file is updated
// instead of compared.
// If `requiredStatusCode` is not 0, the golden file will only be updated if the response has the
// same status code as `requiredStatusCode`. This helps avoid accidentally updating the golden file
// when the test is not working as intended.
func goldenTestHttpRequest(req http.Request, name string, requiredStatusCode int, t *testing.T) {
	path := "testdata/" + name + ".golden.json"

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, &req)

	if *test.UpdateTests && requiredStatusCode != 0 && recorder.Code != requiredStatusCode {
		t.Fatal(t.Name(), ": Expected status code ", requiredStatusCode, " but got ", recorder.Code, " instead. Not updating golden file.")
	}

	golden := struct {
		ResponseCode int
		Headers      http.Header
		Body         json.RawMessage
	}{recorder.Code, recorder.Header(), recorder.Body.Bytes()}

	goldenJSON, err := json.MarshalIndent(golden, "", "    ")
	if err != nil {
		t.Fatal("Failed to marshal golden JSON: ", err)
	}

	test.CompareWithGoldenFile(t, path, string(goldenJSON), *test.UpdateTests)
}

func testHttpResponseCode(req http.Request, expectedStatusCode int, t *testing.T) {
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, &req)
	if recorder.Code != expectedStatusCode {
		t.Fatalf("Expected status code %d, got %d instead", expectedStatusCode, recorder.Code)
	}
}
