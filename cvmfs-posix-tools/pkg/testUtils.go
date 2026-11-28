package pkg

import (
	"bytes"
	"fmt"
	"html/template"
	"os"
	"os/exec"
	"os/user"
	"strconv"
	"strings"
	"time"

	acl "github.com/cvmfs/cvmfs/cvmfs-posix-tools/go-acl"
	pathlib "github.com/chigopher/pathlib"
)

func FullyContainerized() bool {
	skipRepoFuncs, err := strconv.ParseBool(os.Getenv("CVMFS_RSYNC_FULLY_CONTAINERIZED"))
	if err != nil {
		return false
	}
	return skipRepoFuncs
}

func TestMount() string {
	v := os.Getenv("CVMFS_TEST_MOUNTPOINT")
	if (v != "") {
		return v
	}
	if FullyContainerized() {
		return FullyContainerizedTestMount
	}
	return LocalTestMount
}

func TestMountName() string {
	v := os.Getenv("CVMFS_TEST_REPO")
	if (v != "") {
		return v
	}
	if FullyContainerized() {
		return FullyContainerizedTestMountName
	}
	return LocalTestMountName
}

func TestingTempDir() string {
	if FullyContainerized() {
		return FullyContainerizedTempDir
	}
	return ""
}

// Clear cvmfs cache (necessary to avoid cvmfs from using cached version of testing repo)
func ClearCvmfsCache() {
	cmd := exec.Command("rm", "-r", "-f", "/tmp/var/cache/cvmfs/" + TestMountName())
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		panic(err)
	}
}

// Unmount test mount
func UmountRepo() {
	cmd := exec.Command("umount", TestMount())
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		panic(err)
	}
}

// Mount test mount as cvmfs dir
func MountRepo() {
	cmd := exec.Command("mount", TestMount())
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		panic(err)
	}
}

// Stop the testing container (gateway)
func StopContainer() {
	preCmd := exec.Command("podman", "stop", "-i", "cvmfs_rsync_tests")
	preCmd.Stdout = os.Stdout
	preCmd.Stderr = os.Stderr
	if err := preCmd.Run(); err != nil {
		panic(err)
	}
	cmd := exec.Command("podman", "rm", "-f", "-i", "-v", "cvmfs_rsync_tests")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		panic(err)
	}
}

// Build the testing container (gateway)
func BuildContainer() {
	cmd := exec.Command("podman", "build", "-t", "cvmfs_gateway", "../../pkg")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		panic(err)
	}
}

// /start the testing container (gateway)
func StartContainer() {
	cmd := exec.Command("podman", "run", "--cap-add=SYS_PTRACE", "-d", "-p", "8000:8000", "-p", "4929:4929",
		"--name", "cvmfs_rsync_tests", "cvmfs_gateway")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		panic(err)
	}

}

// Setup the testing environment E2E
func SetupEnvironmentE2E() {
	if !FullyContainerized() {
		fmt.Println("UNMOUNTING REPO")
		UmountRepo()
		fmt.Println("STOP")
		StopContainer()
		fmt.Println("CLEAR CACHE")
		ClearCvmfsCache()
		fmt.Println("BUILD")
		BuildContainer()
		fmt.Println("START")
		StartContainer()
		time.Sleep(1000 * time.Millisecond)
		fmt.Println("MOUNT REPO")
		MountRepo()
	}
}

// Mock out getting the repo based on the passed in path
func MockGetRepoPath(path *pathlib.Path) (*pathlib.Path, *pathlib.Path, error) {
	fmt.Println(path)
	var newPath string
	if TestMount() == path.Clean().String() {
		newPath = "."
	} else {
		newPath = path.Clean().String()[len(TestMount())+1:]
	}
	return pathlib.NewPath(TestMount()), pathlib.NewPath(newPath), nil
}

// Set up a config file in a temporary location for use in tests, returning its
// path. Renders a template from the .cvmfs_rsync.yaml in the directory of each
// tool e.g.  cmd/cvmfs_rsync/.cvmfs_rsync.yaml will be used when running tests
// for cvmfs_rsyncry of each tool e.g. cmd/cvmfs_rsync/.cvmfs_rsync.yaml will be
// used when running tests for cvmfs_rsync.
//
// It is the callers responsibility to remove the config file when finished.
func SetupConfigFile() string {
	contentAddressable := false
	for _, arg := range os.Args[1:] {
		if strings.ToLower(arg) == "ca" {
			contentAddressable = true
		}
	}
	u, err := user.Current()
	if err != nil {
		panic(err)
	}
	myUid, err := strconv.Atoi(u.Uid)
	if err != nil {
		panic(err)
	}
	myGid, err := strconv.Atoi(u.Gid)
	if err != nil {
		panic(err)
	}
	myGroup, err := user.LookupGroupId(u.Gid)
	if err != nil {
		panic(err)
	}
	configFile := CVMFSConfigFileOverride
	if contentAddressable {
		configFile = CVMFSCAConfigFileOverride
	}
	testConfFile := pathlib.NewPath(configFile)
	tmpl, err := template.New(testConfFile.Name()).ParseFiles(testConfFile.String())
	if err != nil {
		panic(err)
	}

	var b bytes.Buffer
	err = tmpl.Execute(
		&b,
		struct {
			Uid   int
			Gid   int
			Group string
			Repo  string
		}{Uid: myUid, Gid: myGid, Group: myGroup.Name, Repo: TestMountName()})
	if err != nil {
		panic(err)
	}
	renderedConfFile, err := os.CreateTemp("", "cvmfs-*.yaml")
	if err != nil {
		panic(err)
	}
	if _, err := renderedConfFile.Write(b.Bytes()); err != nil {
		panic(err)
	}
	renderedConfFile.Close()
	return renderedConfFile.Name()
}

// Verify that mode bits and ACL match
func VerifyModeMatchACL(dir *pathlib.Path) bool {
	fileInfo, err := dir.Stat()
	fileMode := fileInfo.Mode() & os.ModePerm
	if err != nil {
		panic(err)
	}
	parsedAcl, err := acl.GetFileAccess(dir.String())
	if err != nil {
		panic(err)
	}
	_, mode, err := parsedAcl.EquivMode()
	return (mode & os.ModePerm) == fileMode
}

func MockDestInCvmfsFromFilePath(filePath *pathlib.Path) (*pathlib.Path, error) {
	absParentPath, err := GetAbsolutePath(filePath.Clean().Parent())
	if err != nil {
		return filePath, err
	}
	var absPath *pathlib.Path
	if filePath.Name() == CurrentDirectory {
		absPath = absParentPath
		absParentPath = absPath.Parent()
	} else if filePath.Name() == PreviousDirectory {
		absPath = absParentPath.Parent()
		absParentPath = absPath.Parent()
	} else {
		absPath = absParentPath.Join(filePath.Name())
	}
	return absPath, nil
}
