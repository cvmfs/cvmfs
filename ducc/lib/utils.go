package lib

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/base64"
	"hash"
	"io"
	"os"
	"strings"

	l "github.com/cvmfs/ducc/log"
	temp "github.com/cvmfs/ducc/temp"
)

type ReadHashCloseSizer interface {
	io.Reader
	hash.Hash
	io.Closer

	GetSize() int64
}

type OnDiskReadAndHash struct {
	*ReadAndHash

	path string
}

// this structure is useful, but each time we use this, we are hogging up the netowrk
// we force to download all the layer, and **then** we check if the layer is already in CVMFS
// this can be optimize
// the constructor, this function, should be smarter.
// it could return immediately while starting a goroutine that does the real downloading and copy work
// on Read we block until the goroutine has not finished
// we still use a lot of network, but we don't wait for it.
// To avoid using the network, on Close() we could close the request body
func NewOnDiskReadAndHash(r io.ReadCloser) (*OnDiskReadAndHash, error) {
	defer r.Close()
	f, err := temp.UserDefinedTempFile()
	if err != nil {
		os.RemoveAll(f.Name())
		return &OnDiskReadAndHash{}, err
	}
	if _, err = io.Copy(f, r); err != nil {
		os.RemoveAll(f.Name())
		return &OnDiskReadAndHash{}, err
	}
	if _, err := f.Seek(0, 0); err != nil {
		os.RemoveAll(f.Name())
		return &OnDiskReadAndHash{}, err
	}
	l.Log().Info("Done downloading")
	readAndHash := NewReadAndHash(f)
	return &OnDiskReadAndHash{ReadAndHash: readAndHash, path: f.Name()}, nil
}

func (r *OnDiskReadAndHash) Close() error {
	if err := os.RemoveAll(r.path); err != nil {
		return err
	}
	return r.ReadAndHash.Close()
}

//encapsulates io.ReadCloser, with functionality to calculate hash and size of the content
type ReadAndHash struct {
	r    io.ReadCloser
	tr   io.Reader
	size int64
	hash hash.Hash
}

func NewReadAndHash(r io.ReadCloser) *ReadAndHash {
	rh := &ReadAndHash{r: r, hash: sha256.New()}
	reader := io.TeeReader(rh.r, rh.hash)
	rh.tr = io.TeeReader(reader, rh)
	return rh
}

func (rh *ReadAndHash) Read(b []byte) (n int, err error) {
	return rh.tr.Read(b)
}

func (rh *ReadAndHash) Write(p []byte) (int, error) {
	n := len(p)
	rh.size += int64(n)
	return n, nil
}

func (rh *ReadAndHash) Sum(data []byte) []byte {
	return rh.Sum256(data)
}

func (rh *ReadAndHash) Sum256(data []byte) []byte {
	return rh.hash.Sum(data)
}

func (rh *ReadAndHash) GetSize() int64 {
	return rh.size
}

func (rh *ReadAndHash) BlockSize() int {
	return sha256.BlockSize
}

func (rh *ReadAndHash) Reset() {
	rh.hash.Reset()
}

func (rh *ReadAndHash) Size() int {
	return sha256.Size
}

func (rh *ReadAndHash) Close() error {
	return rh.r.Close()
}

//generates the file name for link dir in podman store
func generateID(l int) (string, error) {
	randomid := make([]byte, 16)
	n, err := io.ReadFull(rand.Reader, randomid)
	if n != len(randomid) || err != nil {
		return "", err
	}
	s := base32.StdEncoding.EncodeToString(randomid)
	return s[:l], nil
}

//generates the file name for config file (compliant with libpod) in podman store.
func generateConfigFileName(digest string) (fname string, err error) {
	reader := strings.NewReader(digest)
	for reader.Len() > 0 {
		ch, size, err := reader.ReadRune()
		if err != nil || size != 1 {
			break
		}
		if ch != '.' && !(ch >= '0' && ch <= '9') && !(ch >= 'a' && ch <= 'z') {
			break
		}
	}
	if reader.Len() > 0 {
		fname = "=" + base64.StdEncoding.EncodeToString([]byte(digest))
	}
	return
}
