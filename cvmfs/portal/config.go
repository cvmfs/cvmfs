package main

import (
	"encoding/json"
	"io/ioutil"
)

func LoadConfig(configPath string) (config PublisherConfig, err error) {
	var f []byte

	f, err = ioutil.ReadFile(configPath)
	if err != nil {
		return config, err
	}

	err = json.Unmarshal(f, &config)
	if err != nil {
		return config, err
	}

	return config, nil
}
