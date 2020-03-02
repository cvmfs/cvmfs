package dockerutil

import (
	"fmt"
	"strings"
)

type ConfigType struct {
	MediaType string
	Size      int
	Digest    string
}

type Layer struct {
	MediaType string
	Size      int
	Digest    string
}

type Manifest struct {
	SchemaVersion int
	MediaType     string
	Config        ConfigType
	Layers        []Layer
}

type ThinImageLayer struct {
	Digest string `json:"digest"`
	Url    string `json:"url,omitempty"`
}

type ThinImage struct {
	Version    string           `json:"version"`
	MinVersion string           `json:"min_version,omitempty"`
	Origin     string           `json:"origin,omitempty"`
	Layers     []ThinImageLayer `json:"layers"`
	Comment    string           `json:"comment,omitempty"`
}

var thinImageVersion = "1.0"

// m is the manifest of the original image
// repoLocation is where inside the repo we saved the several layers
// origin is an ecoding fo the original referencese and original registry
// I believe origin is quite useless but maybe is better to preserv it for
// ergonomic reasons.
func MakeThinImage(m Manifest, layersMapping map[string]string, origin string) (ThinImage, error) {
	layers := make([]ThinImageLayer, len(m.Layers))

	url_base := "cvmfs://"

	for i, layer := range m.Layers {
		digest := strings.Split(layer.Digest, ":")[1]
		location, ok := layersMapping[layer.Digest]
		if !ok {
			err := fmt.Errorf("Impossible to create thin image, missing layer")
			return ThinImage{}, err
		}
		// the location comes as /cvmfs/$reponame/$path
		// we need to remove the /cvmfs/ part, which are 7 chars
		url := url_base + location[7:]
		layers[i] = ThinImageLayer{Digest: digest, Url: url}
	}

	return ThinImage{Layers: layers,
		Origin:  origin,
		Version: thinImageVersion}, nil
}
