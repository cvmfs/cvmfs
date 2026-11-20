// Package sif converts Apptainer SIF images into tar streams for
// `cvmfs_server ingest`, reading the image through an io.ReaderAt so it can
// live on a remote server.
package sif

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/cvmfs/ducc/sif/squashfs"
)

// Layouts follow github.com/apptainer/sif/v2 pkg/sif (header, rawDescriptor,
// partition). That module is not imported because it pulls in signing and
// registry dependencies that ducc does not need for reading.
type rawHeader struct {
	LaunchScript [32]byte
	Magic        [10]byte
	Version      [3]byte
	Arch         [3]byte
	ID           uuid.UUID

	CreatedAt  int64
	ModifiedAt int64

	DescriptorsFree   int64
	DescriptorsTotal  int64
	DescriptorsOffset int64
	DescriptorsSize   int64
	DataOffset        int64
	DataSize          int64
}

type rawDescriptor struct {
	DataType        int32
	Used            bool
	ID              uint32
	GroupID         uint32
	LinkedID        uint32
	Offset          int64
	Size            int64
	SizeWithPadding int64
	CreatedAt       int64
	ModifiedAt      int64
	UID             int64
	GID             int64
	Name            [128]byte
	Extra           [384]byte
}

const (
	DataDeffile      = 0x4001
	DataEnvVar       = 0x4002
	DataLabels       = 0x4003
	DataPartition    = 0x4004
	DataSignature    = 0x4005
	DataGenericJSON  = 0x4006
	DataGeneric      = 0x4007
	DataCryptoMsg    = 0x4008
	DataSBOM         = 0x4009
	DataOCIRootIndex = 0x400a
	DataOCIBlob      = 0x400b

	FsSquash            = 1
	FsExt3              = 2
	FsImmuObj           = 3
	FsRaw               = 4
	FsEncryptedSquashfs = 5

	PartSystem  = 1
	PartPrimSys = 2
	PartData    = 3
	PartOverlay = 4
)

var (
	sifMagic       = [10]byte{'S', 'I', 'F', '_', 'M', 'A', 'G', 'I', 'C', 0}
	headerSize     = binary.Size(rawHeader{})
	descriptorSize = binary.Size(rawDescriptor{})
	maxDescriptors = int64(1 << 16)
)

var archNames = map[string]string{
	"01": "386", "02": "amd64", "03": "arm", "04": "arm64", "05": "ppc64",
	"06": "ppc64le", "07": "mips", "08": "mipsle", "09": "mips64",
	"10": "mips64le", "11": "s390x", "12": "riscv64", "13": "loong64",
}

type Descriptor struct {
	Type   int32  `json:"type"`
	ID     uint32 `json:"id"`
	Name   string `json:"name,omitempty"`
	Offset int64  `json:"offset"`
	Size   int64  `json:"size"`
	// Digest of an OCI blob ("sha256:HEX"), as recorded by the SIF writer.
	Digest string `json:"digest,omitempty"`
}

// Image is the parsed metadata of a SIF file or bare squashfs image.
type Image struct {
	Bare    bool // bare squashfs, no SIF container
	UUID    string
	Created time.Time
	Arch    string
	Signed  bool
	Descs   []Descriptor
	// OCI is set for an OCI-SIF: the root filesystem is in the layer blobs
	// (see ReadOCI) and PartStart/PartSize are unset.
	OCI       bool
	PartStart int64
	PartSize  int64
	// headerDigest is sha256(global header || descriptor table); empty for
	// bare squashfs images.
	headerDigest string
}

// HeaderDigest is the default image ID: it changes on every rebuild and is
// cheap to compute, but is not a hash of the content.
func (im *Image) HeaderDigest() string { return im.headerDigest }

func cstring(b []byte) string {
	if i := bytes.IndexByte(b, 0); i >= 0 {
		b = b[:i]
	}
	return string(b)
}

// Parse reads the SIF header and descriptors from r and locates the primary
// squashfs partition. A bare squashfs image is accepted too.
func Parse(r io.ReaderAt, size int64) (*Image, error) {
	if size < 4 {
		return nil, fmt.Errorf("image too small (%d bytes)", size)
	}
	var magic [4]byte
	if _, err := r.ReadAt(magic[:], 0); err != nil {
		return nil, fmt.Errorf("reading image magic: %w", err)
	}
	if binary.LittleEndian.Uint32(magic[:]) == squashfs.Magic {
		return &Image{Bare: true, PartStart: 0, PartSize: size}, nil
	}

	if size < int64(headerSize) {
		return nil, fmt.Errorf("not a SIF image: too small")
	}
	hbuf := make([]byte, headerSize)
	if _, err := r.ReadAt(hbuf, 0); err != nil {
		return nil, fmt.Errorf("reading SIF header: %w", err)
	}
	var h rawHeader
	if err := binary.Read(bytes.NewReader(hbuf), binary.LittleEndian, &h); err != nil {
		return nil, err
	}
	if h.Magic != sifMagic {
		return nil, fmt.Errorf("not a SIF or squashfs image")
	}
	if h.DescriptorsTotal <= 0 || h.DescriptorsTotal > maxDescriptors ||
		h.DescriptorsOffset < int64(headerSize) ||
		h.DescriptorsSize != h.DescriptorsTotal*int64(descriptorSize) ||
		h.DescriptorsOffset+h.DescriptorsSize > size {
		return nil, fmt.Errorf("invalid SIF descriptor table (offset %d, %d entries, %d bytes)",
			h.DescriptorsOffset, h.DescriptorsTotal, h.DescriptorsSize)
	}
	dbuf := make([]byte, h.DescriptorsSize)
	if _, err := r.ReadAt(dbuf, h.DescriptorsOffset); err != nil {
		return nil, fmt.Errorf("reading SIF descriptors: %w", err)
	}

	sum := sha256.New()
	sum.Write(hbuf)
	sum.Write(dbuf)
	im := &Image{
		UUID:         h.ID.String(),
		Created:      time.Unix(h.CreatedAt, 0).UTC(),
		headerDigest: hex.EncodeToString(sum.Sum(nil)),
	}

	var primary *rawDescriptor
	var hasOCI bool
	dr := bytes.NewReader(dbuf)
	for i := int64(0); i < h.DescriptorsTotal; i++ {
		var d rawDescriptor
		if err := binary.Read(dr, binary.LittleEndian, &d); err != nil {
			return nil, err
		}
		if !d.Used {
			continue
		}
		if d.Offset < 0 || d.Size < 0 || d.Offset+d.Size > size {
			return nil, fmt.Errorf("SIF descriptor %d: object %d+%d outside image of %d bytes",
				d.ID, d.Offset, d.Size, size)
		}
		desc := Descriptor{Type: d.DataType, ID: d.ID, Name: cstring(d.Name[:]), Offset: d.Offset, Size: d.Size}
		if d.DataType == DataOCIRootIndex || d.DataType == DataOCIBlob {
			desc.Digest = cstring(d.Extra[:])
		}
		im.Descs = append(im.Descs, desc)
		switch d.DataType {
		case DataSignature:
			im.Signed = true
		case DataOCIRootIndex, DataOCIBlob:
			hasOCI = true
		case DataPartition:
			fstype := int32(binary.LittleEndian.Uint32(d.Extra[0:]))
			parttype := int32(binary.LittleEndian.Uint32(d.Extra[4:]))
			if parttype != PartPrimSys {
				continue
			}
			if primary != nil {
				return nil, fmt.Errorf("SIF has more than one primary system partition")
			}
			switch fstype {
			case FsSquash:
			case FsExt3:
				return nil, fmt.Errorf("unsupported SIF partition type: ext3")
			case FsEncryptedSquashfs:
				return nil, fmt.Errorf("unsupported SIF partition type: encrypted squashfs")
			default:
				return nil, fmt.Errorf("unsupported SIF partition fs type %d", fstype)
			}
			dd := d
			primary = &dd
			code := cstring(d.Extra[8:11])
			im.Arch = archNames[code]
			if im.Arch == "" {
				im.Arch = "unknown"
			}
		}
	}
	if primary == nil {
		if hasOCI {
			im.OCI = true
			return im, nil
		}
		return nil, fmt.Errorf("SIF has no primary system partition")
	}
	im.PartStart, im.PartSize = primary.Offset, primary.Size
	return im, nil
}

// Partition returns a reader over the primary squashfs partition.
func (im *Image) Partition(r io.ReaderAt) *io.SectionReader {
	return io.NewSectionReader(r, im.PartStart, im.PartSize)
}

// DescTypeName names a SIF data type for logs and provenance.
func DescTypeName(t int32) string {
	names := map[int32]string{
		DataDeffile: "deffile", DataEnvVar: "envvar", DataLabels: "labels",
		DataPartition: "partition", DataSignature: "signature",
		DataGenericJSON: "generic-json", DataGeneric: "generic",
		DataCryptoMsg: "crypto-message", DataSBOM: "sbom",
		DataOCIRootIndex: "oci-root-index", DataOCIBlob: "oci-blob",
	}
	if n, ok := names[t]; ok {
		return n
	}
	return strings.ToLower(fmt.Sprintf("unknown-%#x", t))
}
