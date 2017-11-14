package main

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
)

func publishRequestHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Got a publish request!")

	var obj PublishingObject
	var err error
	if obj, err = decodePayload(r); err != nil {
		fmt.Println("Failed to parse request.")
		return
	}

	fmt.Printf("Bucket:\t%s\n", obj.Bucket)
	fmt.Printf("Key:\t%s\n", obj.Key)

	publishRequestChan <- obj
	w.WriteHeader(200)
}

func statusRequestHandler(w http.ResponseWriter, r *http.Request) {
	var obj StatusRequest

	vars := mux.Vars(r)

	obj.Key = string(vars["id"])

	statusRequestChan <- obj
	obj = <-statusResponseChan

	if obj.Status == "done" {
		w.Write([]byte("done"))
	} else if obj.Status == "publishing" {
		w.Write([]byte("publishing"))
	} else {
		w.Write([]byte("unknown"))
	}
}
