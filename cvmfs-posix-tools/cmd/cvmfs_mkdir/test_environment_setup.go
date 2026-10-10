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

func addPermissionedDirs() {
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
	hasher := pkg.NewHasher(30, pkg.IOBufferSize)
	fHashData, err := hasher.HashFile(fInfo, pathlib.NewPath(f.Name()), pkg.CVMFSChunkSize)
	if err != nil {
		panic(err)
	}
	// Should probably create an initializer here for the database struct now that it exists
	database, err := pkg.NewCvmfsGraftingDB()
	if err != nil {
		panic(err)
	}

	uid := os.Geteuid()
	gid := os.Getegid()

	database.InsertDir("disallow-upload", 493, time.Now().UnixNano(), uid, gid, "user::rwx,group::r-x,other::r-x")
	database.InsertDir("check-permissions", 365, time.Now().UnixNano(), uid, gid, "user::rwx,group::r-x,other::r-x")
	database.InsertLink("sym_file2.txt", "file2.txt", time.Now().UnixNano(), uid, gid, pkg.EXTERNAL)
	database.InsertLink("sym_file.txt", "file.txt", time.Now().UnixNano(), uid, gid, pkg.EXTERNAL)
	database.InsertFile("file.txt", f.Name(), 0755, time.Now().UnixNano(), uid, gid, fInfo.Size(), strings.Join(pkg.HashesToStrings(fHashData.Hashes), ","), fmt.Sprintf("%040x", fHashData.Checksum), fInfo, pkg.EXTERNAL, false)
	pkg.Mock_graft_getter()(database, "", "", true)
	pkg.UmountRepo()
	time.Sleep(500 * time.Millisecond)
	pkg.MountRepo()
	if err := database.Teardown(true); err != nil {
		panic(err)
	}
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
	if _, err := f.Write([]byte("user::rwx\ngroup::---\nother::---")); err != nil {
		panic(err)
	}
	if err := f.Close(); err != nil {
		panic(err)
	}

	return func(t *testing.T) {
		os.RemoveAll(srcDir)
	}, f.Name()
}
