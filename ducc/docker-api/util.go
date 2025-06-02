package dockerutil

import (
	"fmt"
	"path/filepath"
	"strings"

	digest "github.com/opencontainers/go-digest"
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

type ManifestList struct {
	SchemaVersion int
	MediaType     string
	Manifests     []ManifestListItem
}

type ManifestListItem struct {
	MediaType string
	Size      int
	Digest    string
	Platform  struct {
		Architecture string
		OS           string
		Variant      *string
	}
	Manifest Manifest
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
// origin is an encoding for the original references and original registry
// I believe origin is quite useless but maybe is better to preserve it for
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

func (m Manifest) GetSingularityPath() string {
	digest := strings.Split(m.Config.Digest, ":")[1]
	return filepath.Join(".flat", digest[0:2], digest)
}

// please note how we use the simple digest from the layers, it is not
// strictly correct, since we would need the digest of the uncompressed
// layer, that can be found in the Config file of the image.
// For our purposes, however, this is good enough.
// Foreign layers leave an empty string in the chain list
func (m Manifest) GetChainIDs() []digest.Digest {
	result := []digest.Digest{}
	iChain := 0
	for _, l := range m.Layers {
		if l.MediaType == "application/vnd.docker.image.rootfs.foreign.diff.tar.gzip" {
			result = append(result, "")
			continue
		}
		if iChain == 0 {
			d := digest.FromString(l.Digest)
			result = append(result, d)
			iChain++
			continue
		}
		digest := digest.FromString(result[iChain-1].String() + " " + l.Digest)
		result = append(result, digest)
		iChain++
	}
	return result
}
