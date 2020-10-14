package applyDirectory

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

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

func ApplyDirectory(bottom, top string) error {
	bottomDir, err := os.Stat(bottom)
	if err != nil {
		return err
	}
	if !bottomDir.IsDir() {
		return fmt.Errorf("bottom directory '%s' is not a directory", bottom)
	}

	topDir, err := os.Stat(top)
	if err != nil {
		return err
	}
	if !topDir.IsDir() {
		return fmt.Errorf("top directory '%s' is not a directory", top)
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
			// it may happen a directory that does not exists in the lower layer is an opaqueWhiteout in the top layer
			continue
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
		// delete the file that should be whiteout
		whiteOutBaseFilename := filepath.Base(whiteOut.Path)
		toRemoveBaseFilename := getPathToWhiteout(whiteOutBaseFilename)
		whiteOutPath := filepath.Join(bottom, filepath.Dir(whiteOut.Path), toRemoveBaseFilename)
		err = os.RemoveAll(whiteOutPath)
		if err != nil {
			return err
		}
	}
	for _, file := range standards {
		// add the file or directory
		// remember to set permision and owner
		// if it is a directory, just create the directory with all the permision and the correct owner
		// if is is a file, just create the file and then copy all the content

		// start by getting the UID and GID of the file
		var UID int
		var GID int
		var rdev int
		if stat, ok := file.Info.Sys().(*syscall.Stat_t); ok {
			UID = int(stat.Uid)
			GID = int(stat.Gid)
			rdev = int(stat.Rdev)
		} else {
			UID = os.Getuid()
			GID = os.Getgid()
			rdev = 0
		}
		// then we get the permission
		filemode := file.Info.Mode()
		// and finally we compose the path of the file
		path := filepath.Join(bottom, file.Path)

		switch {
		case filemode.IsDir():
			// we create a very open directory to avoid permission problem
			// we fix all the permission in the defer statements.
			// the defer are stacked, hence the last directory created will
			// be the first one to have its permission changed.
			err = os.MkdirAll(path, 0755)
			if err != nil {
				return err
			}
			defer func(path string, filemode os.FileMode, UID, GID int) {
				os.Chmod(path, filemode)
				os.Chown(path, UID, GID)
			}(path, filemode, UID, GID)

		case filemode.IsRegular():
			// we remove the file, it may not exists and this call will return PathError.
			// we just ignore this kind of error
			os.Remove(path)
			newFile, err := os.Create(path)
			if err != nil {
				return err
			}
			srcFile, err := os.Open(filepath.Join(top, file.Path))
			if err != nil {
				if errors.Is(err, os.ErrPermission) {
					fmt.Printf("Permission error detected! %s", err)
				}
				newFile.Close()
				return err
			}
			_, err = io.Copy(newFile, srcFile)
			srcFile.Close()
			if err != nil {
				newFile.Close()
				return err
			}

			if filemode&os.ModeSetuid != 0 || filemode&os.ModeSetgid != 0 {
				fmt.Println("setxid on: ", filemode, " on file: ", filepath.Join(top, file.Path))
			}

			err = newFile.Chmod(filemode)
			if err != nil {
				fmt.Println("Error in chmod: ", err)
			}
			err = newFile.Chown(UID, GID)
			if err != nil {
				fmt.Println("Error in chown: ", err)
			}
			newStat, _ := newFile.Stat()
			if newStat.Mode() != filemode {
				fmt.Println("Different mode in the new file! ", newStat.Mode(), " -vs- ", filemode)
			}
			newFile.Close()

		case filemode&os.ModeSymlink != 0:
			os.Remove(path)
			link, err := os.Readlink(filepath.Join(top, file.Path))
			if err != nil {
				return err
			}

			err = os.Symlink(link, path)
			if err != nil {
				return err
			}
			os.Lchown(path, UID, GID)

		case filemode&os.ModeNamedPipe != 0:
			fallthrough
		case filemode&os.ModeSocket != 0:
			err = unix.Mkfifo(path, uint32(filemode))
			if err != nil {
				return err
			}
			os.Chown(path, UID, GID)
		case filemode&os.ModeDevice != 0:
			err = unix.Mknod(path, uint32(filemode), rdev)
			if err != nil {
				return err
			}
			os.Chown(path, UID, GID)
		}

	}
	return nil
}

// simulate the --fix-perms flags in singularity
// best effort, we swallow all the errors beside path not existing
func FixPerms(path string) error {
	if _, err := os.Stat(path); err != nil {
		return err
	}
	UID := os.Getuid()
	GID := os.Getgid()

	filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		os.Lchown(path, UID, GID)
		if info.Mode().IsDir() {
			os.Chmod(path, info.Mode().Perm()|0700)
		} else {
			os.Chmod(path, info.Mode().Perm()|0600)
		}
		return nil
	})
	return nil
}
