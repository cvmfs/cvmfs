package pkg

import (
	"fmt"
	"os"
	"testing"

	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/go-acl"
	pathlib "github.com/chigopher/pathlib"
	"gotest.tools/v3/assert"
)

func TestReadDirExclude(t *testing.T) {
	tests := []struct {
		name        string
		excludes    []string
		dirContents []string
		want        []string
	}{
		{
			"basic single exclude",
			[]string{"file1.txt"},
			[]string{"file1.txt", "file2.txt", "file3.txt"},
			[]string{"file2.txt", "file3.txt"},
		},
		{
			"basic dir exclude",
			[]string{"dir1"},
			[]string{"dir1/file1.txt", "dir1/file2.txt", "dir2/file3.txt"},
			[]string{"dir2"},
		},
		{
			"basic subdir exclude",
			[]string{"dir1/"},
			[]string{"dir1/file1.txt", "dir1/file2.txt", "dir2/file3.txt"},
			[]string{"dir1", "dir2"},
		},
		{
			"basic multiple exclude",
			[]string{"file1.txt", "file2.txt"},
			[]string{"file1.txt", "file2.txt", "file3.txt"},
			[]string{"file3.txt"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tempDir, err := os.MkdirTemp("", "")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(tempDir)
			rootDir := pathlib.NewPath(tempDir)

			for _, x := range test.dirContents {
				p := rootDir.Join(x)
				assert.NilError(t, p.MkdirAll())
				assert.NilError(t, err)
			}

			cfg := ConfStruct{}
			paths, err := ReadDirExclude(cfg, rootDir, true, test.excludes...)
			assert.NilError(t, err)
			comparePathNames(t, rootDir, paths, test.want)
		})
	}

}

func TestGetAclString_NoACL(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "")
	assert.NilError(t, err)
	defer os.RemoveAll(tempDir)
	rootDir := pathlib.NewPath(tempDir)
	f := rootDir.Join("file.txt")
	assert.NilError(t, f.WriteFile([]byte("foo")))

	aclString, err := GetAclString(f.String())
	assert.NilError(t, err)
	assert.Equal(t, aclString, "")
}

func TestGetAclString_AclEquivalentToFilePerm(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "")
	assert.NilError(t, err)
	defer os.RemoveAll(tempDir)
	rootDir := pathlib.NewPath(tempDir)
	f := rootDir.Join("file.txt")
	assert.NilError(t, f.WriteFile([]byte("foo")))

	a, err := acl.Parse("user::rwx,group::rwx,other::rwx")
	assert.NilError(t, err)
	defer a.Free()
	assert.NilError(t, a.SetFileAccess(f.String()))

	aclString, err := GetAclString(f.String())
	assert.NilError(t, err)
	assert.Equal(t, aclString, "")
}

func TestGetAclString_ActualAcl(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "")
	assert.NilError(t, err)
	defer os.RemoveAll(tempDir)
	rootDir := pathlib.NewPath(tempDir)
	f := rootDir.Join("file.txt")
	assert.NilError(t, f.WriteFile([]byte("foo")))

	// Annoyingly, from a testing POV, this requires a valid ACL on the current
	// system (users/groups must exist) so root is being used as it is very
	// likely to be present wherever this test is being run
	a, err := acl.Parse("g:root:rw,u:root:rw,u::wr,g::r,o::r,m::r")
	assert.NilError(t, err)
	defer a.Free()
	assert.NilError(t, a.SetFileAccess(f.String()))

	aclString, err := GetAclString(f.String())
	assert.NilError(t, err)
	fmt.Println(aclString)
	assert.Equal(t, aclString, "user::rw-\nuser:0:rw-\ngroup::r--\ngroup:0:rw-\nmask::r--\nother::r--")
}

func comparePathNames(t *testing.T, rootDir *pathlib.Path, paths []*pathlib.Path, expectedNames []string) {
	pathNames := make([]string, len(paths))
	for i, p := range paths {
		p, err := p.RelativeTo(rootDir)
		assert.NilError(t, err)
		pathNames[i] = p.String()
	}
	assert.DeepEqual(t, pathNames, expectedNames)
}
