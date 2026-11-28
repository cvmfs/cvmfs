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

func setupTestEnv(ctx Context) (func(t *testing.T), string, pkg.FileHashData) {
	srcDir, err := os.MkdirTemp(pkg.TestingTempDir(), "cvmfs_test_src_dir")
	if err != nil {
		panic(err)
	}

	f, err := os.Create(srcDir + "/test_file.txt")
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
	fStat, err := os.Lstat(f.Name())
	if err != nil {
		panic(err)
	}
	fHashData, err := hasher.HashFile(fStat, pathlib.NewPath(f.Name()), pkg.CVMFSChunkSize)
	if err != nil {
		panic(err)
	}

	uid := os.Geteuid()
	gid := os.Getegid()

	// TODO: Can probably extract the file/dir/link creator into test_util file
	noPermDirs := []string{
		"no_own_dir",
		"no_own_dir2",
		"check-permissions/no_own_dir",
		"check-permissions/no_own_dir2",
	}
	dirsToCreate := []string{
		"check-permissions",
		"dir",
		"dir/inner_dir",
		"dir/inner_dir2",
		"dir/inner_dir3",
		"dir/inner_dir3/inner_dir",
		"dir/inner_dir3.1",
		"dir/inner_dir3.1/inner_dir",
		"dir/inner_dir3.2",
		"dir/inner_dir3.2/inner_dir",
		"dir/inner_dir4",
		"dir/inner_dir5",
		"dir/inner_dir5/inner_dir",
		"dir/inner_dir6",
	}
	noPermFiles := []string{
		"no_perm.txt",
		"no_perm2.txt",
		"check-permissions/no_perm.txt",
		"check-permissions/no_perm2.txt",
	}
	filesToCreate := []string{
		"file.txt",
		"file2.txt",
		"file3.txt",
		"file4.txt",
		"dir/file.txt",
		"dir/file2.txt",
		"dir/file3.txt",
		"dir/inner_dir/file.txt",
		"dir/inner_dir2/file.txt",
		"dir/inner_dir3/file.txt",
		"dir/inner_dir3.1/file.txt",
		"dir/inner_dir3.2/file.txt",
		"dir/inner_dir4/file.txt",
		"dir/inner_dir5/file.txt",
		"dir/inner_dir6/file.txt",
	}
	noPermLinks := []SymTargetDest{
		SymTargetDest{Dest: "no_own_dir2_sym", Target: "no_own_dir2"},
		SymTargetDest{Dest: "no_perm2_sym.txt", Target: "no_perm2.txt"},
		SymTargetDest{Dest: "check-permissions/no_own_dir2_sym", Target: "no_own_dir2"},
		SymTargetDest{Dest: "check-permissions/no_perm2_sym.txt", Target: "no_perm2.txt"},
	}
	linksToCreate := []SymTargetDest{
		SymTargetDest{Dest: "sym_file.txt", Target: "file.txt"},
		SymTargetDest{Dest: "sym_file2.txt", Target: "file2.txt"},
		SymTargetDest{Dest: "dir/sym_file.txt", Target: "file.txt"},
		SymTargetDest{Dest: "dir/sym_file2.txt", Target: "file2.txt"},
		SymTargetDest{Dest: "dir/sym_file3.txt", Target: "file3.txt"},
		SymTargetDest{Dest: "dir/inner_dir2/sym_file.txt", Target: "file.txt"},
		SymTargetDest{Dest: "dir/inner_dir3/sym_file.txt", Target: "file.txt"},
		SymTargetDest{Dest: "dir/inner_dir3.1/sym_file.txt", Target: "file.txt"},
		SymTargetDest{Dest: "dir/inner_dir3.2/sym_file.txt", Target: "file.txt"},
		SymTargetDest{Dest: "dir/inner_dir4/sym_file.txt", Target: "file.txt"},
		SymTargetDest{Dest: "dir/inner_dir5/sym_file.txt", Target: "file.txt"},
		SymTargetDest{Dest: "dir/inner_dir6/sym_file.txt", Target: "file.txt"},
	}

	for _, dirPathString := range dirsToCreate {
		if err := db.InsertDir(dirPathString, 0777, time.Now().UnixNano(), uid, gid, "user::rwx,group::r-x,other::r-x"); err != nil {
			panic(err)
		}
	}
	for _, dirPathString := range noPermDirs {
		if err := db.InsertDir(dirPathString, 0777, time.Now().UnixNano(), pkg.TestingUnownedUGid, gid, "user::rwx,group::r-x,other::r-x"); err != nil {
			panic(err)
		}
	}

	if ctx.cfg.Repo.DotScheme {
		for _, filePathString := range noPermFiles {
			filePath := pathlib.NewPath(filePathString)
			if err := db.InsertLink(filePathString, "."+filePath.Name()+"."+fmt.Sprintf("%040x", fHashData.Checksum), time.Now().UnixNano(), pkg.TestingUnownedUGid, gid, pkg.EXTERNAL); err != nil {
				panic(err)
			}
			filePathParts := filePath.Parts()
			dotFilePathString := strings.Join(append(filePathParts[0:len(filePathParts)-1], "."+filePath.Name()+"."+fmt.Sprintf("%040x", fHashData.Checksum)), pkg.FileDelimeter)
			if err := db.InsertFile(dotFilePathString, f.Name(), 0777, time.Now().UnixNano(), pkg.TestingUnownedUGid, gid, fInfo.Size(), strings.Join(pkg.HashesToStrings(fHashData.Hashes), ","), fmt.Sprintf("%040x", fHashData.Checksum), fInfo, pkg.EXTERNAL, false); err != nil {
				panic(err)
			}
		}
		for _, filePathString := range filesToCreate {
			filePath := pathlib.NewPath(filePathString)
			if err := db.InsertLink(filePathString, "."+filePath.Name()+"."+fmt.Sprintf("%040x", fHashData.Checksum), time.Now().UnixNano(), uid, gid, pkg.EXTERNAL); err != nil {
				panic(err)
			}
			filePathParts := filePath.Parts()
			dotFilePathString := strings.Join(append(filePathParts[0:len(filePathParts)-1], "."+filePath.Name()+"."+fmt.Sprintf("%040x", fHashData.Checksum)), pkg.FileDelimeter)
			if err := db.InsertFile(dotFilePathString, f.Name(), 0777, time.Now().UnixNano(), uid, gid, fInfo.Size(), strings.Join(pkg.HashesToStrings(fHashData.Hashes), ","), fmt.Sprintf("%040x", fHashData.Checksum), fInfo, pkg.EXTERNAL, false); err != nil {
				panic(err)
			}
		}
	} else {
		for _, filePathString := range noPermFiles {
			if err := db.InsertFile(filePathString, f.Name(), 0777, time.Now().UnixNano(), pkg.TestingUnownedUGid, gid, fInfo.Size(), strings.Join(pkg.HashesToStrings(fHashData.Hashes), ","), fmt.Sprintf("%040x", fHashData.Checksum), fInfo, pkg.EXTERNAL, false); err != nil {
				panic(err)
			}
		}
		for _, filePathString := range filesToCreate {
			if err := db.InsertFile(filePathString, f.Name(), 0777, time.Now().UnixNano(), uid, gid, fInfo.Size(), strings.Join(pkg.HashesToStrings(fHashData.Hashes), ","), fmt.Sprintf("%040x", fHashData.Checksum), fInfo, pkg.EXTERNAL, false); err != nil {
				panic(err)
			}
		}
	}
	for _, symTD := range noPermLinks {
		if err := db.InsertLink(symTD.Dest, symTD.Target, time.Now().UnixNano(), pkg.TestingUnownedUGid, gid, pkg.EXTERNAL); err != nil {
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
	}, f.Name(), fHashData
}
