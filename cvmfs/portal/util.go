package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

func decodePayload(r *http.Request) (obj PublishingObject, err error) {
	buf := new(bytes.Buffer)
	buf.ReadFrom(r.Body)

	var payload WebhookPayload
	if err := json.Unmarshal(buf.Bytes(), &payload); err != nil {
		fmt.Println(buf.String())
		return obj, err
	}

	obj = payload.Object()
	return obj, nil
}
