package lib

import (
	"bytes"
	"encoding/base32"
	"encoding/base64"
	"io"
	"io/ioutil"
	"path"
	"strings"

	"github.com/google/uuid"
)

// this flag is populated in the main `rootCmd` (cmd/root.go)
var (
	TemporaryBaseDir string
)

//function that satisfies io.Closer.
type CloseFunc func() error

type readCloser struct {
	io.Reader
	c CloseFunc
}

func (r readCloser) Close() error {
	return r.c()
}

//wrapper around bytes.Buffer which implements close methods too.
type ReadCloserBuffer struct {
	*bytes.Buffer
}

func (cb ReadCloserBuffer) Close() (err error) {
	return
}

func UserDefinedTempDir(dir, prefix string) (name string, err error) {
	if strings.HasPrefix(dir, TemporaryBaseDir) {
		return ioutil.TempDir(dir, prefix)
	}
	return ioutil.TempDir(path.Join(TemporaryBaseDir, dir), prefix)
}

// TeeReadCloser returns a io.ReadCloser that writes everything it reads from r to w.
// The Close method for the returned ReadCloser is same as that of r.
func TeeReadCloser(r io.ReadCloser, w io.Writer) io.ReadCloser {
	return readCloser{
		io.TeeReader(r, w),
		r.Close,
	}
}

//generates the file name for link dir in podman store
func generateID(l int) string {
	id := uuid.New()
	bytes, _ := id.MarshalText()
	s := base32.StdEncoding.EncodeToString(bytes)

	return s[:l]
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
