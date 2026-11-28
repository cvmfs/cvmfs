package main

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	pathlib "github.com/chigopher/pathlib"
	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
)

type SymTargetDest struct {
	Dest   string
	Target string
}

func setupTestEnv() (func(t *testing.T), string) {
	srcDir, err := os.MkdirTemp(pkg.TestingTempDir(), "cvmfs_test_src_dir")
	if err != nil {
		panic(err)
	}

	f, err := os.CreateTemp(srcDir, "test_file.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f.Write([]byte{1, 2, 3, 4}); err != nil {
		panic(err)
	}
	if err := f.Close(); err != nil {
		panic(err)
	}
	fInfo, err := os.Lstat(f.Name())
	if err != nil {
		panic(err)
	}

	db, err := pkg.NewCvmfsGraftingDB()
	if err != nil {
		panic(err)
	}

	hasher := pkg.NewHasher(30, pkg.IOBufferSize)
	fHashData, err := hasher.HashFile(fInfo, pathlib.NewPath(f.Name()), pkg.CVMFSChunkSize)
	if err != nil {
		panic(err)
	}

	uid := os.Geteuid()
	gid := os.Getegid()

	limitedPermDirs := []string{
		"check-permissions",
	}
	dirsToCreate := []string{
		"check-permissions2",
		"disallow-delete",
		"dir",
		"dir/inner_dir",
		"dir/inner_dir2",
		"dir/inner_dir3",
		"dir/inner_dir4",
	}
	filesToCreate := []string{
		"check-permissions/file.txt",
		"disallow-delete/file.txt",
		"file.txt",
		"file2.txt",
		".dot-file.txt." + fmt.Sprintf("%040x", fHashData.Checksum),
		"dir/file.txt",
		"dir/file2.txt",
		"dir/file3.txt",
		"dir/inner_dir2/file.txt",
		"dir/inner_dir3/file.txt",
	}
	linksToCreate := []SymTargetDest{
		SymTargetDest{Dest: "dot-file.txt", Target: ".dot-file.txt." + fmt.Sprintf("%040x", fHashData.Checksum)},
		SymTargetDest{Dest: "sym_file.txt", Target: "file.txt"},
		SymTargetDest{Dest: "sym_file_broken.txt", Target: "paulson.txt"},
		SymTargetDest{Dest: "sym_file2.txt", Target: "file2.txt"},
		SymTargetDest{Dest: "dir/sym_file.txt", Target: "file.txt"},
		SymTargetDest{Dest: "dir/sym_file2.txt", Target: "file2.txt"},
		SymTargetDest{Dest: "dir/sym_file3.txt", Target: "file3.txt"},
		SymTargetDest{Dest: "dir/inner_dir2/sym_file.txt", Target: "file.txt"},
		SymTargetDest{Dest: "dir/inner_dir3/sym_file.txt", Target: "file.txt"},
	}
	for _, dirPathString := range limitedPermDirs {
		if err := db.InsertDir(dirPathString, 0555, time.Now().UnixNano(), uid, gid, "user::r-x,group::r-x,other::r-x"); err != nil {
			panic(err)
		}
	}
	for _, dirPathString := range dirsToCreate {
		if err := db.InsertDir(dirPathString, 0755, time.Now().UnixNano(), uid, gid, "user::rwx,group::r-x,other::r-x"); err != nil {
			panic(err)
		}
	}

	for _, filePathString := range filesToCreate {
		if err := db.InsertFile(filePathString, f.Name(), 0755, time.Now().UnixNano(), uid, gid, fInfo.Size(), strings.Join(pkg.HashesToStrings(fHashData.Hashes), ","), fmt.Sprintf("%040x", fHashData.Checksum), fInfo, pkg.EXTERNAL, false); err != nil {
			panic(err)
		}
	}

	for _, symTD := range linksToCreate {
		if err := db.InsertLink(symTD.Dest, symTD.Target, time.Now().UnixNano(), uid, gid, pkg.EXTERNAL); err != nil {
			panic(err)
		}
	}

	pkg.Mock_graft_getter()(db, "", "", true)
	pkg.UmountRepo()
	time.Sleep(500 * time.Millisecond)
	pkg.MountRepo()
	return func(t *testing.T) {
		os.RemoveAll(srcDir)
	}, ".dot-file.txt." + fmt.Sprintf("%040x", fHashData.Checksum)
}
