package main

import "net/http"
import "fmt"
import "os"

import "github.com/gorilla/mux"

var cm CvmfsManager

var publisherConfig PublisherConfig

var publishRequestChan = make(chan PublishingObject)
var statusRequestChan = make(chan StatusRequest)
var statusResponseChan = make(chan StatusRequest)
var backendChan = make(chan PublishingObject, 32)
var controlChan = make(chan string)

func main() {
	if print_info() {
		return
	}

	if len(os.Args) < 2 {
		fmt.Println("Specify path to config file as first argument.")
		os.Exit(-1)
	}

	var err error
	if publisherConfig, err = LoadConfig(os.Args[1]); err != nil {
		fmt.Println("Invalid config.")
		fmt.Println(err)
		os.Exit(-2)
	}
	fmt.Println("Running shuttle service on " + publisherConfig.Fqrn +
							publisherConfig.CvmfsPathPrefix + "@" +
							fmt.Sprintf("%d", publisherConfig.Port))

	cm = CvmfsManager{fqrn: publisherConfig.Fqrn,
		cvmfsPathPrefix: publisherConfig.CvmfsPathPrefix}

	go frontendWorker()
	go backendWorker()

	m := mux.NewRouter()
	m.HandleFunc("/", publishRequestHandler)
	m.HandleFunc("/status/{id}", statusRequestHandler)
	http.Handle("/", m)
	http.ListenAndServe("127.0.0.1:" + fmt.Sprintf("%d", publisherConfig.Port), nil)
}
