package lib

import (
	"io/ioutil"
	"path"
)

// this flag is populated in the main `rootCmd` (cmd/root.go)
var (
	TemporaryBaseDir string
)

func UserDefinedTempDir(dir, prefix string) (name string, err error) {
	return ioutil.TempDir(path.Join(TemporaryBaseDir, dir), prefix)
}
