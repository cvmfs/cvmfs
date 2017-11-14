package main

import (
	"fmt"
	"path"
	"sort"
)

func backendWorker() {
	for {
		obj := <-backendChan

		if err := cm.StartTransaction(); err != nil {
			fmt.Println(err)
			cm.AbortTransaction()
			controlChan <- obj.Key
			continue
		}

		filepath := path.Join(publisherConfig.localPayloadPath, obj.Bucket, obj.Key)
		if err := cm.ImportTarball(filepath, obj.Key); err != nil {
			fmt.Println(err)
			cm.AbortTransaction()
			controlChan <- obj.Key
			continue
		}

		if err := cm.PublishTransaction(); err != nil {
			fmt.Println(err)
			cm.AbortTransaction()
			controlChan <- obj.Key
			continue
		}
		controlChan <- obj.Key
	}
}

func frontendWorker() {
	// var m string
	var buffer []string

	for {
		select {
		case req := <-publishRequestChan:
			buffer = append(buffer, req.Key)
			sort.Strings(buffer)
			fmt.Printf("Got new publishing request: %v\n", req.Key)
			backendChan <- req
		case key := <-controlChan:
			fmt.Printf("Got update, layer %s published!", key)
			if i := sort.SearchStrings(buffer, key); i < len(buffer) {
				buffer = append(buffer[:i], buffer[i+1:]...)
			}
		case req := <-statusRequestChan:
			pos := sort.SearchStrings(buffer, req.Key)
			if pos < len(buffer) && buffer[pos] == req.Key {
				req.Status = "publishing"
			} else if cm.LookupLayer(req.Key) {
				fmt.Printf("Found published layer %v\n", req.Key)
				req.Status = "done"
			} else {
				fmt.Printf("layer %v lookup failed\n", req.Key)
				req.Status = "unknown"
			}

			statusResponseChan <- req
		}
	}

}
