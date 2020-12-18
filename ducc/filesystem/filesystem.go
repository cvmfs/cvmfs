package filesystem

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/iafan/cwalk"
	log "github.com/sirupsen/logrus"

	l "github.com/cvmfs/ducc/log"
)

func init() {
	// we need a lot more of goroutines when scanning CVMFS directories
	cwalk.NumWorkers = runtime.GOMAXPROCS(0) * 10
	cwalk.BufferSize = cwalk.NumWorkers
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

	log := l.Log().WithFields(log.Fields{"top layer": top, "bottom": bottom})

	log.Info("Start walking the top layer")

	var opaqueMtx sync.Mutex
	var opaqueWhiteouts []string

	var whiteMtx sync.Mutex
	var whiteOuts []string

	var standardMtx sync.Mutex
	var standards []FilePathAndInfo

	// navigating the filesystem in slow, especially on top of CVMFS
	// for each file we need to go through fuse and to a network call if we are unlucky
	// we make this walk in parallel, so to hide the latency
	err = cwalk.Walk(top, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		path = strings.Replace(path, top, "", 1)
		if isOpaqueWhiteout(path) {
			opaqueMtx.Lock()
			defer opaqueMtx.Unlock()
			opaqueWhiteouts = append(opaqueWhiteouts, path)
		} else if isWhiteout(path) {
			whiteMtx.Lock()
			defer whiteMtx.Unlock()
			whiteOuts = append(whiteOuts, path)
		} else {
			standardMtx.Lock()
			defer standardMtx.Unlock()
			standards = append(standards, FilePathAndInfo{path, info})
		}
		return nil
	})

	if err != nil {
		return err
	}

	log.Info("Done walking the top layer")

	log.Info("Start sorting the files")

	// it is necessary to sort everything since the parallel walk returns unsorted data
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		sort.Strings(opaqueWhiteouts)
	}()
	go func() {
		defer wg.Done()
		sort.Strings(whiteOuts)
	}()
	go func() {
		defer wg.Done()
		sort.Slice(standards,
			func(i, j int) bool { return standards[i].Path < standards[j].Path })
	}()
	wg.Wait()

	log.Info("Done sorting the files")

	log.Info("Start applying the opaque whiteouts")

	// all the call here happens against a local and fast filesystem
	// we are not going to make effort to make this parallel
	for _, opaqueWhiteout := range opaqueWhiteouts {
		// delete all the sibling of the opaque whiteout file file
		opaqueDirPath := filepath.Join(bottom, filepath.Dir(opaqueWhiteout))
		inOpaqueDirectory, err := ioutil.ReadDir(opaqueDirPath)
		if err != nil {
			// it may happen a directory that does not exists in the lower layer
			// is an opaqueWhiteout in the top layer
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

	log.Info("Done applying the opaque whiteouts")

	log.Info("Start applying the whiteouts")

	for _, whiteOut := range whiteOuts {
		// delete the file that should be whiteout
		whiteOutBaseFilename := filepath.Base(whiteOut)
		toRemoveBaseFilename := getPathToWhiteout(whiteOutBaseFilename)
		whiteOutPath := filepath.Join(bottom, filepath.Dir(whiteOut), toRemoveBaseFilename)
		err = os.RemoveAll(whiteOutPath)
		if err != nil {
			return err
		}
	}

	log.Info("Done applying the whiteouts")

	log.Info("Start applying the files on top of the bottom dir")

	for _, file := range standards {
		// add the file or directory
		// remember to set permision and owner
		// If it is a directory, just create the directory with
		//     all the permision and the correct owner
		// If is is a file, just create the file and then copy all the content

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
			err = os.MkdirAll(path, 0700)
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
				if err == os.ErrPermission {
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
			newFile.Chown(UID, GID)
			newFile.Chmod(filemode)
			newFile.Close()

		case filemode&os.ModeSymlink != 0:
			os.RemoveAll(path)
			link, err := os.Readlink(filepath.Join(top, file.Path))
			if err != nil {
				return err
			}

			// links can be either absolutes or relative
			// if they are absolute we need to swap the top prefix with the bottom one
			// if they are relative they are good to go
			// even though we should pretty much have alway relative links
			if strings.HasPrefix(link, top) {
				link = strings.Replace(link, top, bottom, 1)
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
	log.Info("Done applying the files on top of the bottom dir")
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
