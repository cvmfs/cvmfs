package lib

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/base64"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
)

// this flag is populated in the main `rootCmd` (cmd/root.go)
var (
	TemporaryBaseDir string
)

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

func (rh *ReadAndHash) Sum256(data []byte) []byte {
	return rh.hash.Sum(data)
}

func (rh *ReadAndHash) GetSize() int64 {
	return rh.size
}

func (rh *ReadAndHash) Close() error {
	return rh.r.Close()
}

func UserDefinedTempDir(dir, prefix string) (name string, err error) {
	if strings.HasPrefix(dir, TemporaryBaseDir) {
		return ioutil.TempDir(dir, prefix)
	}
	return ioutil.TempDir(path.Join(TemporaryBaseDir, dir), prefix)
}

func UserDefinedTempFile() (f *os.File, err error) {
	return ioutil.TempFile(TemporaryBaseDir, "write_data")
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
