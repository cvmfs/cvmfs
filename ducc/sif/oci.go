package sif

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// MediaTypeSquashfsLayer is the layer type SingularityCE writes into
// OCI-SIFs (`singularity pull --oci`, `build --oci`).
const MediaTypeSquashfsLayer = "application/vnd.sylabs.image.layer.v1.squashfs"

const (
	mediaTypeDockerManifest = "application/vnd.docker.distribution.manifest.v2+json"
	maxOCIJSON              = 4 << 20
)

// OCIImage is the image embedded in an OCI-SIF.
type OCIImage struct {
	IndexDigest    string        `json:"index_digest"`
	ManifestDigest string        `json:"manifest_digest"`
	ConfigDigest   string        `json:"config_digest"`
	Layers         []OCILayer    `json:"layers"`
	Config         ocispec.Image `json:"-"`
}

type OCILayer struct {
	Digest string `json:"digest"`
	Size   int64  `json:"size"`
	Offset int64  `json:"-"`
}

// ReadOCI reads the root index, manifest and config of an OCI-SIF. Their
// digests are verified, so the config digest identifies the image as far
// as the layer digests can be trusted; the layers themselves are not
// hashed, as that would need a full extra read of the image.
func (im *Image) ReadOCI(r io.ReaderAt) (*OCIImage, error) {
	if !im.OCI {
		return nil, fmt.Errorf("not an OCI-SIF image")
	}
	var root *Descriptor
	blobs := map[string]*Descriptor{}
	for i := range im.Descs {
		d := &im.Descs[i]
		switch d.Type {
		case DataOCIRootIndex:
			if root != nil {
				return nil, fmt.Errorf("OCI-SIF has more than one root index")
			}
			root = d
		case DataOCIBlob:
			blobs[d.Digest] = d
		}
	}
	if root == nil {
		return nil, fmt.Errorf("OCI-SIF has no root index")
	}
	read := func(d *Descriptor, digest string, size int64, v interface{}) error {
		if d.Size > maxOCIJSON {
			return fmt.Errorf("OCI blob %s: %d bytes is too large", digest, d.Size)
		}
		if size >= 0 && d.Size != size {
			return fmt.Errorf("OCI blob %s: size %d, descriptor says %d", digest, d.Size, size)
		}
		data := make([]byte, d.Size)
		if _, err := r.ReadAt(data, d.Offset); err != nil {
			return fmt.Errorf("reading OCI blob %s: %w", digest, err)
		}
		sum := sha256.Sum256(data)
		if got := "sha256:" + hex.EncodeToString(sum[:]); got != digest {
			return fmt.Errorf("OCI blob %s: content has digest %s", digest, got)
		}
		if err := json.Unmarshal(data, v); err != nil {
			return fmt.Errorf("OCI blob %s: %w", digest, err)
		}
		return nil
	}
	blob := func(desc ocispec.Descriptor) (*Descriptor, error) {
		d := blobs[string(desc.Digest)]
		if d == nil {
			return nil, fmt.Errorf("OCI-SIF lacks blob %s (%s)", desc.Digest, desc.MediaType)
		}
		return d, nil
	}

	o := &OCIImage{IndexDigest: root.Digest}
	var index ocispec.Index
	if err := read(root, root.Digest, -1, &index); err != nil {
		return nil, fmt.Errorf("root index: %w", err)
	}
	if len(index.Manifests) != 1 {
		return nil, fmt.Errorf("OCI-SIF root index lists %d manifests, want 1", len(index.Manifests))
	}
	md := index.Manifests[0]
	// SingularityCE keeps the Docker media type of images pulled from a
	// Docker registry; the manifest schema is the same.
	if md.MediaType != ocispec.MediaTypeImageManifest && md.MediaType != mediaTypeDockerManifest {
		return nil, fmt.Errorf("OCI-SIF root index points to %s, want an image manifest", md.MediaType)
	}
	d, err := blob(md)
	if err != nil {
		return nil, err
	}
	var manifest ocispec.Manifest
	if err := read(d, string(md.Digest), md.Size, &manifest); err != nil {
		return nil, fmt.Errorf("manifest: %w", err)
	}
	o.ManifestDigest = string(md.Digest)

	if d, err = blob(manifest.Config); err != nil {
		return nil, err
	}
	if err := read(d, string(manifest.Config.Digest), manifest.Config.Size, &o.Config); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	o.ConfigDigest = string(manifest.Config.Digest)

	if len(manifest.Layers) == 0 {
		return nil, fmt.Errorf("OCI-SIF image has no layers")
	}
	if len(manifest.Layers) > maxLayers {
		return nil, fmt.Errorf("OCI-SIF image has %d layers, at most %d are supported", len(manifest.Layers), maxLayers)
	}
	for _, l := range manifest.Layers {
		if l.MediaType != MediaTypeSquashfsLayer {
			return nil, fmt.Errorf("unsupported OCI-SIF layer type %s", l.MediaType)
		}
		if d, err = blob(l); err != nil {
			return nil, err
		}
		if d.Size != l.Size {
			return nil, fmt.Errorf("OCI layer %s: size %d, manifest says %d", l.Digest, d.Size, l.Size)
		}
		o.Layers = append(o.Layers, OCILayer{string(l.Digest), d.Size, d.Offset})
	}
	return o, nil
}

// Layer returns a reader over layer i.
func (o *OCIImage) Layer(r io.ReaderAt, i int) *io.SectionReader {
	return io.NewSectionReader(r, o.Layers[i].Offset, o.Layers[i].Size)
}
