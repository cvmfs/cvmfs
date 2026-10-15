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

	noPermDirs := []string{
		"check-permissions",
		"check-permissions/no_own_dir",
		"no_own_dir",
		"no_own_dir2",
		"no_own_dir3",
		"no_own_dir4",
	}
	dirsToCreate := []string{
		"dir",
		"dir2",
		"dir3",
		"dir4",
		"check-permissions/no_own_dir/dir",
		"check-permissions/no_own_dir/dir2",
	}
	noPermFiles := []string{
		"no_perm.txt",
		"no_perm2.txt",
		"no_perm3.txt",
		"no_perm4.txt",
	}
	filesToCreate := []string{
		"file.txt",
		"file2.txt",
		"file3.txt",
		"file4.txt",
		"file5.txt",
		"file6.txt",
		"check-permissions/no_own_dir/file.txt",
		"check-permissions/no_own_dir/file2.txt",
	}
	noPermSyms := []SymTargetDest{
		SymTargetDest{Dest: "no_own_sym_file3.txt", Target: "no_perm3.txt"},
		SymTargetDest{Dest: "no_own_sym_dir3", Target: "no_own_dir3"},
		SymTargetDest{Dest: "no_own_sym_file4.txt", Target: "no_perm4.txt"},
		SymTargetDest{Dest: "no_own_sym_dir4", Target: "no_own_dir4"},
	}
	linksToCreate := []SymTargetDest{
		SymTargetDest{Dest: "sym_file2.txt", Target: "file2.txt"},
		SymTargetDest{Dest: "sym_dir2", Target: "dir2"},
		SymTargetDest{Dest: "sym_file6.txt", Target: "file6.txt"},
		SymTargetDest{Dest: "sym_dir4", Target: "dir4"},
		SymTargetDest{Dest: "check-permissions/no_own_dir/sym_file2.txt", Target: "file2.txt"},
		SymTargetDest{Dest: "check-permissions/no_own_dir/sym_dir2", Target: "dir2"},
	}

	for _, dirPathString := range noPermDirs {
		if err := db.InsertDir(dirPathString, 0755, time.Now().UnixNano(), pkg.TestingUnownedUGid, gid, "user::rwx,group::r-x,other::r-x"); err != nil {
			panic(err)
		}
	}
	for _, dirPathString := range dirsToCreate {
		if err := db.InsertDir(dirPathString, 0755, time.Now().UnixNano(), uid, gid, "user::rwx,group::r-x,other::r-x"); err != nil {
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
			if err := db.InsertFile(dotFilePathString, f.Name(), 0755, time.Now().UnixNano(), pkg.TestingUnownedUGid, gid, fInfo.Size(), strings.Join(pkg.HashesToStrings(fHashData.Hashes), ","), fmt.Sprintf("%040x", fHashData.Checksum), fInfo, pkg.EXTERNAL, false); err != nil {
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
			if err := db.InsertFile(dotFilePathString, f.Name(), 0755, time.Now().UnixNano(), uid, gid, fInfo.Size(), strings.Join(pkg.HashesToStrings(fHashData.Hashes), ","), fmt.Sprintf("%040x", fHashData.Checksum), fInfo, pkg.EXTERNAL, false); err != nil {
				panic(err)
			}
		}
	} else {
		for _, filePathString := range noPermFiles {
			if err := db.InsertFile(filePathString, f.Name(), 0755, time.Now().UnixNano(), pkg.TestingUnownedUGid, gid, fInfo.Size(), strings.Join(pkg.HashesToStrings(fHashData.Hashes), ","), fmt.Sprintf("%040x", fHashData.Checksum), fInfo, pkg.EXTERNAL, false); err != nil {
				panic(err)
			}
		}
		for _, filePathString := range filesToCreate {
			if err := db.InsertFile(filePathString, f.Name(), 0755, time.Now().UnixNano(), uid, gid, fInfo.Size(), strings.Join(pkg.HashesToStrings(fHashData.Hashes), ","), fmt.Sprintf("%040x", fHashData.Checksum), fInfo, pkg.EXTERNAL, false); err != nil {
				panic(err)
			}
		}
	}
	for _, symTD := range noPermSyms {
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
