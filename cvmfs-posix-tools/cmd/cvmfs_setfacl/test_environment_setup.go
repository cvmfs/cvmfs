package main

import (
	"fmt"
	"os"
	"os/user"
	"strconv"
	"strings"
	"testing"
	"time"

	pathlib "github.com/chigopher/pathlib"
	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
)

func setupTestEnv() (func(t *testing.T), string, string, string, string, string, string, string) {
	uid := os.Geteuid()
	gid := os.Getegid()
	groupObj, err := user.LookupGroupId(strconv.Itoa(gid))
	if err != nil {
		panic(err)
	}
	groupname := groupObj.Name

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

	f2, err := os.CreateTemp(srcDir, "test_file2.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f2.Write([]byte(fmt.Sprintf("user::rwx\nuser:%d:---\ngroup::---\nmask::---\nother::---", pkg.TestingUnownedUGid))); err != nil {
		panic(err)
	}
	if err := f2.Close(); err != nil {
		panic(err)
	}
	f3, err := os.CreateTemp(srcDir, "test_file3.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f3.Write([]byte(fmt.Sprintf("user::rwx\ngroup::---\ngroup:%d:r-x\nmask::r-x\nother::---", gid))); err != nil {
		panic(err)
	}
	if err := f3.Close(); err != nil {
		panic(err)
	}
	f4, err := os.CreateTemp(srcDir, "test_file4.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f4.Write([]byte(fmt.Sprintf("user::rwx\ngroup::---\ngroup:%d:r-x\nmask::r-x\nother::---", pkg.TestingUnownedUGid))); err != nil {
		panic(err)
	}
	if err := f4.Close(); err != nil {
		panic(err)
	}
	f5, err := os.CreateTemp(srcDir, "test_file5.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f5.Write([]byte(fmt.Sprintf("user::rwx\ngroup::---\ngroup:%d:r-x\ngroup:%d:r-x\nmask::r-x\nother::---", pkg.TestingUnownedUGid, gid))); err != nil {
		panic(err)
	}
	if err := f5.Close(); err != nil {
		panic(err)
	}

	f6, err := os.CreateTemp(srcDir, "test_file6.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f6.Write([]byte(fmt.Sprintf("# comment\nuser::rwx\ngroup::---\ngroup:%d:r-x\ngroup:%d:r-x\ndefault:group:%d:r-x\nmask::r-x\nother::---", pkg.TestingUnownedUGid, gid, gid))); err != nil {
		panic(err)
	}
	if err := f6.Close(); err != nil {
		panic(err)
	}

	f7, err := os.CreateTemp(srcDir, "test_file7.txt")
	if err != nil {
		panic(err)
	}
	if _, err := f7.Write([]byte(fmt.Sprintf("user::rwx\ngroup::---\ngroup:%d:r-x # inline comment\ngroup:%d:r-x\nmask::r-x\nother::---", pkg.TestingUnownedUGid, gid))); err != nil {
		panic(err)
	}
	if err := f7.Close(); err != nil {
		panic(err)
	}

	db, err := pkg.NewCvmfsGraftingDB()
	if err != nil {
		panic(err)
	}

	if err := db.InsertDir("check-permissions", 0755, time.Now().UnixNano(), uid, gid, "user::rwx,group::r-x,other::r-x"); err != nil {
		panic(err)
	}
	if err := db.InsertDir("check-permissions/no_own_dir", 0755, time.Now().UnixNano(), pkg.TestingUnownedUGid, gid, "user::rwx,group::r-x,other::r-x"); err != nil {
		panic(err)
	}
	if err := db.InsertDir("no_own_dir", 0755, time.Now().UnixNano(), pkg.TestingUnownedUGid, gid, "user::rwx,group::r-x,other::r-x"); err != nil {
		panic(err)
	}
	if err := db.InsertDir("dir", 0755, time.Now().UnixNano(), uid, gid, "user::rwx,group::r-x,other::r-x"); err != nil {
		panic(err)
	}
	if err := db.InsertDir("dir/inner_dir", 0755, time.Now().UnixNano(), uid, gid, "user::rwx,group::r-x,other::r-x"); err != nil {
		panic(err)
	}
	fNoFacl, err := os.CreateTemp(srcDir, "test_file_no_facl.txt")
	if err != nil {
		panic(err)
	}
	if _, err := fNoFacl.Write([]byte("Some Content")); err != nil {
		panic(err)
	}
	if err := fNoFacl.Close(); err != nil {
		panic(err)
	}
	fNoFaclInfo, err := os.Lstat(fNoFacl.Name())
	if err != nil {
		panic(err)
	}
	hasher := pkg.NewHasher(30, pkg.IOBufferSize)
	fNoFaclHashData, err := hasher.HashFile(fNoFaclInfo, pathlib.NewPath(f.Name()), pkg.CVMFSChunkSize)
	if err != nil {
		panic(err)
	}
	if err := db.InsertLink("dir/no_facl_link.txt", "no_facl.txt", time.Now().UnixNano(), uid, gid, pkg.EXTERNAL); err != nil {
		panic(err)
	}
	if err := db.InsertLink("dir/no_facl_link_dir", "inner_dir", time.Now().UnixNano(), uid, gid, pkg.EXTERNAL); err != nil {
		panic(err)
	}
	if err := db.InsertFile("dir/no_facl.txt", f.Name(), 0755, time.Now().UnixNano(), uid, gid, fNoFaclInfo.Size(), strings.Join(pkg.HashesToStrings(fNoFaclHashData.Hashes), ","), fmt.Sprintf("%040x", fNoFaclHashData.Checksum), fNoFaclInfo, pkg.EXTERNAL, false); err != nil {
		panic(err)
	}
	if err := db.InsertDir("2dir", 0755, time.Now().UnixNano(), uid, gid, "user::rwx,group::r-x,other::r-x"); err != nil {
		panic(err)
	}
	if err := db.InsertDir("2dir/inner_dir", 0755, time.Now().UnixNano(), uid, gid, "user::rwx,group::r-x,other::r-x"); err != nil {
		panic(err)
	}
	if err := db.InsertDir("3dir", 0755, time.Now().UnixNano(), uid, gid, "user::rwx,group::r-x,other::r-x"); err != nil {
		panic(err)
	}
	if err := db.InsertDir("4dir", 0755, time.Now().UnixNano(), uid, gid, "user::rwx,group::r-x,other::r-x"); err != nil {
		panic(err)
	}
	if err := db.InsertDir("5dir", 0755, time.Now().UnixNano(), uid, gid, "user::rwx,group::r-x,other::r-x"); err != nil {
		panic(err)
	}
	if err := db.InsertDir("dir/inner_dir2", 0755, time.Now().UnixNano(), uid, gid, "user::rwx,group::r-x,other::r-x"); err != nil {
		panic(err)
	}
	if err := db.InsertDir("6dir", 0700, time.Now().UnixNano(), uid, gid, "user::rwx,group::---,other::---"); err != nil {
		panic(err)
	}
	if err := db.InsertDir("6.5dir", 0700, time.Now().UnixNano(), uid, gid, "user::rwx,group::---,other::---"); err != nil {
		panic(err)
	}
	if err := db.InsertDir("7dir", 0700, time.Now().UnixNano(), uid, gid, fmt.Sprintf("user::rwx,group::---,group:%s:r-x,mask::r-x,other::---", groupname)); err != nil {
		panic(err)
	}
	if err := db.InsertDir("7.5dir", 0700, time.Now().UnixNano(), uid, gid, fmt.Sprintf("user::rwx,group::---,group:%s:r-x,mask::r-x,other::---", groupname)); err != nil {
		panic(err)
	}
	if err := db.InsertDir("8dir", 0700, time.Now().UnixNano(), uid, gid, fmt.Sprintf("user::rwx,user:%s:r-x,group::---,group:%s:r-x,mask::r-x,other::---", pkg.TestingUnownedUserGroup, groupname)); err != nil {
		panic(err)
	}
	if err := db.InsertDir("9dir", 0700, time.Now().UnixNano(), uid, gid, "user::rwx,group::---,other::---"); err != nil {
		panic(err)
	}
	if err := db.InsertDir("9dir/inner_dir", 0700, time.Now().UnixNano(), uid, gid, fmt.Sprintf("user::rwx,group::---,group:%s:r-x,mask::r-x,other::---", pkg.TestingUnownedUserGroup)); err != nil {
		panic(err)
	}
	if err := db.InsertDir("10dir", 0700, time.Now().UnixNano(), uid, gid, fmt.Sprintf("user::rwx,group::---,group:%s:r-x,mask::r-x,other::---", groupname)); err != nil {
		panic(err)
	}
	if err := db.InsertDir("10dir/inner_dir", 0700, time.Now().UnixNano(), uid, gid, fmt.Sprintf("user::rwx,group::---,group:%s:r-x,group:%s:r-x,mask::r-x,other::---", pkg.TestingUnownedUserGroup, groupname)); err != nil {
		panic(err)
	}
	if err := db.InsertDir("11dir", 0700, time.Now().UnixNano(), uid, gid, fmt.Sprintf("user::rwx,user:%s:r-x,group::---,group:%s:r-x,mask::r-x,other::---", pkg.TestingUnownedUserGroup, groupname)); err != nil {
		panic(err)
	}
	if err := db.InsertDir("11dir/inner_dir", 0700, time.Now().UnixNano(), uid, gid, fmt.Sprintf("user::rwx,user:%s:r-x,group::---,group:%s:r-x,group:%s:r-x,mask::r-x,other::---", pkg.TestingUnownedUserGroup, pkg.TestingUnownedUserGroup, groupname)); err != nil {
		panic(err)
	}
	if err := db.InsertDir("12dir", 0755, time.Now().UnixNano(), uid, gid, "user::rwx,group::r-x,other::r-x"); err != nil {
		panic(err)
	}
	if err := db.InsertDir("12dir/inner_dir", 0755, time.Now().UnixNano(), uid, gid, "user::rwx,group::r-x,other::r-x"); err != nil {
		panic(err)
	}
	if err := db.InsertDir("14dir", 0755, time.Now().UnixNano(), uid, gid, "user::rwx,group::r-x,other::r-x"); err != nil {
		panic(err)
	}
	if err := db.InsertDir("14dir/inner_dir", 0755, time.Now().UnixNano(), uid, gid, "user::rwx,group::r-x,other::r-x"); err != nil {
		panic(err)
	}
	if err := db.InsertDir("15dir", 0755, time.Now().UnixNano(), uid, gid, "user::rwx,group::r-x,other::r-x"); err != nil {
		panic(err)
	}
	if err := db.InsertDir("16dir", 0755, time.Now().UnixNano(), uid, gid, "user::rwx,group::r-x,other::r-x"); err != nil {
		panic(err)
	}
	if err := db.InsertDir("17dir", 0755, time.Now().UnixNano(), uid, gid, "user::rwx,group::r-x,other::r-x"); err != nil {
		panic(err)
	}

	pkg.Mock_graft_getter()(db, "", "", true)
	pkg.UmountRepo()
	time.Sleep(500 * time.Millisecond)
	pkg.MountRepo()
	return func(t *testing.T) {
		os.RemoveAll(srcDir)
	}, f.Name(), f2.Name(), f3.Name(), f4.Name(), f5.Name(), f6.Name(), f7.Name()
}
