package lib

import (
	"io/ioutil"
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
