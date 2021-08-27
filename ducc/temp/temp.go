package temp

import (
	"io/ioutil"
	"os"
	"path"
	"strings"
)

// this flag is populated in the main `rootCmd` (cmd/root.go)
var (
	TemporaryBaseDir string
)

func UserDefinedTempDir(dir, prefix string) (name string, err error) {
	if strings.HasPrefix(dir, TemporaryBaseDir) {
		return ioutil.TempDir(dir, prefix)
	}
	return ioutil.TempDir(path.Join(TemporaryBaseDir, dir), prefix)
}

func UserDefinedTempFile() (f *os.File, err error) {
	return ioutil.TempFile(TemporaryBaseDir, "write_data")
}
