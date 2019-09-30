package lib

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

func ApplyDirectory(bottom, top string) error {
	bottomDir, err := os.Stat(bottom)
	if err != nil {
		return err
	}
	if bottomDir.IsDir() == false {
		return fmt.Errorf("Bottom directory '%s' is not a directory", bottom)
	}

	topDir, err := os.Stat(top)
	if err != nil {
		return err
	}
	if topDir.IsDir() == false {
		return fmt.Errorf("Top directory '%s' is not a directory", top)
	}

	var opaqueWhiteouts []FilePathAndInfo
	var whiteOuts []FilePathAndInfo
	var standards []FilePathAndInfo

	err = filepath.Walk(top, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		path = strings.Replace(path, top, "", 1)
		if isOpaqueWhiteout(path) {
			opaqueWhiteouts = append(opaqueWhiteouts, FilePathAndInfo{path, info})
		} else if isWhiteout(path) {
			whiteOuts = append(whiteOuts, FilePathAndInfo{path, info})
		} else {
			standards = append(standards, FilePathAndInfo{path, info})
		}
		return nil
	})

	if err != nil {
		return err
	}

	for _, opaqueWhiteout := range opaqueWhiteouts {
		// delete all the sibling of the opaque whiteout file file
		opaqueDirPath := filepath.Join(bottom, filepath.Dir(opaqueWhiteout.Path))
		inOpaqueDirectory, err := ioutil.ReadDir(opaqueDirPath)
		if err != nil {
			return err
		}
		for _, toDelete := range inOpaqueDirectory {
			toDeletePath := filepath.Join(opaqueDirPath, toDelete.Name())
			err = os.RemoveAll(toDeletePath)
			if err != nil {
				return err
			}
		}
	}
	for _, whiteOut := range whiteOuts {
		fmt.Println(whiteOut)
		// delete the file that should be whiteout
	}
	for _, file := range standards {
		fmt.Println(file)
		// add the file or directory
	}
	return nil
}

type FilePathAndInfo struct {
	Path string
	Info os.FileInfo
}

func isWhiteout(path string) bool {
	base := filepath.Base(path)
	if len(base) <= 3 {
		return false
	}
	return base[0:4] == ".wh."
}

func isOpaqueWhiteout(path string) bool {
	base := filepath.Base(path)
	return base == ".wh..wh..opq"
}

func getPathToWhiteout(path string) string {
	if !isWhiteout(path) {
		return ""
	}
	if isOpaqueWhiteout(path) {
		return ""
	}
	return strings.TrimPrefix(path, ".wh.")
}
