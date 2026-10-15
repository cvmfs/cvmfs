package main

import (
	"errors"
	"fmt"
	"os"
	"os/user"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/go-acl"
	pathlib "github.com/chigopher/pathlib"
	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
	"github.com/rs/zerolog/log"
)

type AclTestPath struct {
	pathString  string
	expectedAcl string
	implicit    bool
}

type Test struct {
	name         string
	ctx          Context
	aclTestPaths []AclTestPath
	shouldError  bool
	noChange     bool
}

func compareAcls(expectedAclString string, path *pathlib.Path) bool {
	fullPathString := path.Clean().String()
	if !pkg.VerifyModeMatchACL(path) {
		fmt.Printf("%s mode and ACL mismatch\n", fullPathString)
		return false
	}
	a, err := acl.GetFileAccess(fullPathString)
	if err != nil {
		if errors.Is(err, syscall.EOPNOTSUPP) {
			log.Debug().Msg("Failure reading ACL - assuming source is nfsv4")
		} else {
			log.Error().Err(err).Str("Path", fullPathString).Msg("Failed to get FACL for Path")
			panic(err)
		}
	}
	defer a.Free()
	aclstring := ""
	if a != nil {
		aclstring = a.StringWithOptions(acl.TextNumericIDs)
	}
	expectedAcl, err := acl.Parse(expectedAclString)
	if err != nil {
		log.Error().Err(err).Str("Path", fullPathString).Msg("Failed to parse expected FACL")
		panic(err)
	}
	defer expectedAcl.Free()
	cleanExpectedAclString := a.StringWithOptions(acl.TextNumericIDs)
	if cleanExpectedAclString != aclstring {
		fmt.Printf("%s ACL mismatch\n", fullPathString)
		fmt.Println("Expected:")
		fmt.Println(cleanExpectedAclString)
		fmt.Println("Actual:")
		fmt.Println(aclstring)
		return false
	}
	return true
}

func setfaclVerification(expectedAcl string, path *pathlib.Path) bool {
	pathInfo, err := os.Lstat(path.Clean().String())
	if err != nil {
		panic(err)
	}
	if pathlib.IsDir(pathInfo.Mode()) {
		if !compareAcls(expectedAcl, path) {
			return false
		}
	}
	return true
}

func verifySetfacl(ctx Context, aclTestPaths []AclTestPath) bool {
	for _, aclTestPath := range aclTestPaths {
		if !setfaclVerification(aclTestPath.expectedAcl, pathlib.NewPath(aclTestPath.pathString)) {
			return false
		}
	}
	return true
}

func setupContext(recursive bool, faclfile string, modify, remove, removeAll bool, modifyString string) Context {
	ctx := Context{
		recursive:    recursive,
		debug:        true,
		faclFile:     faclfile,
		modifySet:    modify,
		removeSet:    remove,
		modifyString: modifyString,
		removeAll:    removeAll,
	}

	var err error
	ctx.cfg, _, ctx.uid, _, err = pkg.GetCvmfsConfigurationInfo(
		pkg.TestMountName(),
		pathlib.NewPath(OVERRIDE_CONFIG_PATH),
	)
	if err != nil {
		panic(err)
	}

	return ctx
}

func setupExternalFuncs() func() {
	graftHold := graft
	graft = pkg.Mock_graft_getter()
	destInCvmfsFromFilePathHold := destInCvmfsFromFilePath
	destInCvmfsFromFilePath = pkg.MockDestInCvmfsFromFilePath
	getRepoPathHold := getRepoPath
	getRepoPath = pkg.MockGetRepoPath
	return func() {
		graft = graftHold
		destInCvmfsFromFilePath = destInCvmfsFromFilePathHold
		getRepoPath = getRepoPathHold
	}
}

func TestTurboSpeed(t *testing.T) {
	t1, td1 := E2ESetfaclHelper(t, false)
	defer td1(t)
	testCollections := [][][]Test{t1}
	testCollectionLen := len(t1)
	reinstateExternalFuncs := setupExternalFuncs()

	for i := 0; i < testCollectionLen; i++ {
		megaGraftDb, err := pkg.NewCvmfsGraftingDB()
		if err != nil {
			panic(err)
		}
		graft = pkg.Mock_mega_graft(megaGraftDb)
		if i == 1 { // 1 is reserved for no change
			graft = pkg.Mock_no_graft
		}
		for _, testCollection := range testCollections {
			if len(testCollection) > i {
				for _, tc := range testCollection[i] {
					fmt.Printf("\nRunning test %s\n", tc.name)
					paths := []*pathlib.Path{}
					for _, aclTestPath := range tc.aclTestPaths {
						if !aclTestPath.implicit {
							paths = append(paths, pathlib.NewPath(aclTestPath.pathString))
						}
					}
					err := launchSetfacl(tc.ctx, paths)
					if tc.shouldError {
						if err == nil {
							t.Fatalf("Test should have errored.")
						}
					} else {
						if err != nil {
							panic(err)
						}
					}
				}
			}
		}
		if i == 1 {
			graft = pkg.Mock_mega_graft(megaGraftDb)
		}
		pkg.Mock_graft_getter()(megaGraftDb, "", "", true)
		if err := megaGraftDb.Teardown(true); err != nil {
			panic(err)
		}
		fmt.Println("Unmount Repo")
		pkg.UmountRepo()
		time.Sleep(500 * time.Millisecond)
		fmt.Println("Mount Repo")
		pkg.MountRepo()
		fmt.Println("Verifying")
		for _, testCollection := range testCollections {
			if len(testCollection) > i {
				for _, tc := range testCollection[i] {
					if !tc.noChange && !tc.shouldError {
						if !verifySetfacl(tc.ctx, tc.aclTestPaths) {
							t.Fatalf("Filesystems not the same after rsync. Check for yourself.")
						}
					}
				}
			}
		}
	}
	reinstateExternalFuncs()
}

func fastTestRunner(t *testing.T, tests [][]Test) {
	for _, tList := range tests {
		for _, tc := range tList {
			fmt.Printf("\nRunning test %s\n", tc.name)
			if tc.noChange {
				graft = pkg.Mock_no_graft
			}
			paths := []*pathlib.Path{}
			for _, aclTestPath := range tc.aclTestPaths {
				if !aclTestPath.implicit {
					paths = append(paths, pathlib.NewPath(aclTestPath.pathString))
				}
			}
			err := launchSetfacl(tc.ctx, paths)
			if tc.shouldError {
				if err == nil {
					t.Fatalf("Test should have errored.")
				}
			} else {
				if err != nil {
					panic(err)
				}
			}
			if tc.noChange {
				graft = pkg.Mock_graft_getter()
			}
		}
		fmt.Println("Unmount Repo")
		pkg.UmountRepo()
		time.Sleep(500 * time.Millisecond)
		fmt.Println("Mount Repo")
		pkg.MountRepo()
		fmt.Println("Verifying")
		for _, tc := range tList {
			if !tc.shouldError {
				if !verifySetfacl(tc.ctx, tc.aclTestPaths) {
					fmt.Println(tc.name + " failed")

					t.Fatalf("Some mode was not as it should be. Check test mount.")
				}
			}
		}
	}
}

// Note: This assumes no specific users, and is designed almost explicitly for testing
func addAclGroup(faclString, group string) string {
	idx := strings.Index(faclString, "mask")
	if idx == -1 {
		idx = strings.Index(faclString, "other")
	}
	return faclString[:idx] + group + "\n" + faclString[idx:]
}

// Note: This assumes no specific users, and is designed almost explicitly for testing
func modAclMask(faclString, mask string) string {
	print(faclString)
	faclLines := strings.Split(faclString, "\n")
	workingFaclLines := []string{}
	for _, faclLine := range faclLines {
		if !strings.Contains(faclLine, "mask") {
			workingFaclLines = append(workingFaclLines, faclLine)
		}
	}
	workingFaclString := strings.Join(workingFaclLines, "\n")
	idx := strings.Index(workingFaclString, "other")
	return workingFaclString[:idx] + mask + "\n" + workingFaclString[idx:]
}

// Note: This assumes no specific users, and is designed almost explicitly for testing
func removeMatchingAclEntrys(faclString string, removeStrs []string) string {
	faclLines := strings.Split(faclString, "\n")
	workingFaclLines := []string{}
	for _, faclLine := range faclLines {
		removeIt := false
		for _, removeStr := range removeStrs {
			removeIt = removeIt || strings.Contains(faclLine, removeStr)
		}
		if !removeIt {
			workingFaclLines = append(workingFaclLines, faclLine)
		}
	}
	return strings.Join(workingFaclLines, "\n")
}

// Note: This assumes no specific users, and is designed almost explicitly for testing
func removesAllNonStandardAclEntrys(faclString string) string {
	faclLines := strings.Split(faclString, "\n")
	workingFaclLines := []string{}
	for _, faclLine := range faclLines {
		if strings.Contains(faclLine, "::") {
			workingFaclLines = append(workingFaclLines, faclLine)
		}
	}
	return strings.Join(workingFaclLines, "\n")
}

// Note: This assumes no specific users, and is designed almost explicitly for testing
func cleanAclEntrys(faclString string) string {
	faclLines := strings.Split(faclString, "\n")
	workingFaclLines := []string{}
	for _, faclLine := range faclLines {
		if !strings.Contains(faclLine, "#") && !strings.Contains(faclLine, "default") {
			workingFaclLines = append(workingFaclLines, faclLine)
		}
	}
	return strings.Join(workingFaclLines, "\n")
}

func TestE2ESetfacl(t *testing.T) {
	E2ESetfaclHelper(t, true)
}

func E2ESetfaclHelper(t *testing.T, runTest bool) ([][]Test, func(t *testing.T)) {
	gid := os.Getegid()
	groupObj, err := user.LookupGroupId(strconv.Itoa(gid))
	if err != nil {
		panic(err)
	}
	groupname := groupObj.Name

	tearDown, refFile, refFile2, refFile3, refFile4, refFile5, refFile6, refFile7 := setupTestEnv()
	if runTest {
		defer tearDown(t)
	}

	refFileExpectedAcl, err := pkg.GetAclFromFile(refFile)
	if err != nil {
		panic(err)
	}

	refFileExpectedAcl2, err := pkg.GetAclFromFile(refFile2)
	if err != nil {
		panic(err)
	}

	refFileExpectedAcl3, err := pkg.GetAclFromFile(refFile3)
	if err != nil {
		panic(err)
	}

	refFileExpectedAcl4, err := pkg.GetAclFromFile(refFile4)
	if err != nil {
		panic(err)
	}

	refFileExpectedAcl5, err := pkg.GetAclFromFile(refFile5)
	if err != nil {
		panic(err)
	}

	refFileExpectedAcl6, err := pkg.GetAclFromFile(refFile6)
	if err != nil {
		panic(err)
	}

	// t.Parallel()
	// setupEnvironmentE2E()
	tests := [][]Test{
		{
			{name: "Setfacl of dir", ctx: setupContext(false, refFile, false, false, false, ""), aclTestPaths: []AclTestPath{{pathString: pkg.TestMount() + "/dir", expectedAcl: refFileExpectedAcl, implicit: false}}, shouldError: false},
			{name: "Setfacl of another dir", ctx: setupContext(false, refFile, false, false, false, ""), aclTestPaths: []AclTestPath{{pathString: pkg.TestMount() + "/dir/inner_dir", expectedAcl: refFileExpectedAcl, implicit: false}}, shouldError: false},
			{name: "Setfacl recursive", ctx: setupContext(true, refFile, false, false, false, ""), aclTestPaths: []AclTestPath{{pathString: pkg.TestMount() + "/2dir", expectedAcl: refFileExpectedAcl, implicit: false}, {pathString: pkg.TestMount() + "/2dir/inner_dir", expectedAcl: refFileExpectedAcl, implicit: true}}, shouldError: false},
			{name: "Setfacl of multiple dirs", ctx: setupContext(false, refFile, false, false, false, ""), aclTestPaths: []AclTestPath{{pathString: pkg.TestMount() + "/3dir", expectedAcl: refFileExpectedAcl, implicit: false}, {pathString: pkg.TestMount() + "/4dir", expectedAcl: refFileExpectedAcl, implicit: false}}, shouldError: false},
			{name: "Setfacl with real ACL", ctx: setupContext(false, refFile2, false, false, false, ""), aclTestPaths: []AclTestPath{{pathString: pkg.TestMount() + "/5dir", expectedAcl: refFileExpectedAcl2, implicit: false}}, shouldError: false},
			{name: "Cannot setfacl of not owned dir", ctx: setupContext(false, refFile, false, false, false, ""), aclTestPaths: []AclTestPath{{pathString: pkg.TestMount() + "/check-permissions/no_own_dir", expectedAcl: refFileExpectedAcl, implicit: false}}, shouldError: true},
			{name: "Can setfacl of not owned dir, no perm check", ctx: setupContext(false, refFile, false, false, false, ""), aclTestPaths: []AclTestPath{{pathString: pkg.TestMount() + "/no_own_dir", expectedAcl: refFileExpectedAcl, implicit: false}}, shouldError: false},
			{name: "Bad modded acl error", ctx: setupContext(false, refFile, true, false, false, "badacl"), aclTestPaths: []AclTestPath{{pathString: pkg.TestMount() + "/5.5dir", expectedAcl: "", implicit: false}}, shouldError: true},
			{name: "Mod facl of dir", ctx: setupContext(false, refFile, true, false, false, fmt.Sprintf("g:%s:rx", groupname)), aclTestPaths: []AclTestPath{{pathString: pkg.TestMount() + "/6dir", expectedAcl: modAclMask(addAclGroup(refFileExpectedAcl, fmt.Sprintf("group:%d:r-x", gid)), "mask::r-x"), implicit: false}}, shouldError: false},
			{name: "Mod facl of dir (gid)", ctx: setupContext(false, refFile, true, false, false, fmt.Sprintf("g:%d:rx", gid)), aclTestPaths: []AclTestPath{{pathString: pkg.TestMount() + "/6.5dir", expectedAcl: modAclMask(addAclGroup(refFileExpectedAcl, fmt.Sprintf("group:%d:r-x", gid)), "mask::r-x"), implicit: false}}, shouldError: false},
			{name: "Remove facl of dir", ctx: setupContext(false, refFile, false, true, false, fmt.Sprintf("g:%s:rx", groupname)), aclTestPaths: []AclTestPath{{pathString: pkg.TestMount() + "/7dir", expectedAcl: modAclMask(removeMatchingAclEntrys(refFileExpectedAcl3, []string{fmt.Sprintf("group:%d", gid)}), "mask::---"), implicit: false}}, shouldError: false},
			{name: "Remove facl of dir", ctx: setupContext(false, refFile, false, true, false, fmt.Sprintf("g:%s", groupname)), aclTestPaths: []AclTestPath{{pathString: pkg.TestMount() + "/7.5dir", expectedAcl: modAclMask(removeMatchingAclEntrys(refFileExpectedAcl3, []string{fmt.Sprintf("group:%d", gid)}), "mask::---"), implicit: false}}, shouldError: false},
			{name: "Remove all facls of dir", ctx: setupContext(false, refFile, false, false, true, ""), aclTestPaths: []AclTestPath{{pathString: pkg.TestMount() + "/8dir", expectedAcl: modAclMask(removesAllNonStandardAclEntrys(refFileExpectedAcl3), "mask::---"), implicit: false}}, shouldError: false},

			{name: "Mod facl of dir recursive", ctx: setupContext(true, refFile, true, false, false, fmt.Sprintf("g:%s:rx", groupname)), aclTestPaths: []AclTestPath{{pathString: pkg.TestMount() + "/9dir", expectedAcl: modAclMask(addAclGroup(refFileExpectedAcl, fmt.Sprintf("group:%d:r-x", gid)), "mask::r-x"), implicit: false}, {pathString: pkg.TestMount() + "/9dir/inner_dir", expectedAcl: modAclMask(addAclGroup(refFileExpectedAcl4, fmt.Sprintf("group:%d:r-x", gid)), "mask::r-x"), implicit: true}}, shouldError: false},
			{name: "Remove facl of dir recursive", ctx: setupContext(true, refFile, false, true, false, fmt.Sprintf("g:%s:rx", groupname)), aclTestPaths: []AclTestPath{{pathString: pkg.TestMount() + "/10dir", expectedAcl: modAclMask(removeMatchingAclEntrys(refFileExpectedAcl3, []string{fmt.Sprintf("group:%d", gid)}), "mask::---"), implicit: false}, {pathString: pkg.TestMount() + "/10dir/inner_dir", expectedAcl: modAclMask(removeMatchingAclEntrys(refFileExpectedAcl5, []string{fmt.Sprintf("group:%d", gid)}), "mask::r-x"), implicit: true}}, shouldError: false},
			{name: "Remove all facls of dir recursive", ctx: setupContext(true, refFile, false, false, true, ""), aclTestPaths: []AclTestPath{{pathString: pkg.TestMount() + "/11dir", expectedAcl: modAclMask(removesAllNonStandardAclEntrys(refFileExpectedAcl3), "mask::---"), implicit: false}, {pathString: pkg.TestMount() + "/11dir/inner_dir", expectedAcl: modAclMask(removesAllNonStandardAclEntrys(refFileExpectedAcl5), "mask::---"), implicit: true}}, shouldError: false},

			{name: "Setfacl of dir/.", ctx: setupContext(false, refFile, false, false, false, ""), aclTestPaths: []AclTestPath{{pathString: pkg.TestMount() + "/12dir/.", expectedAcl: refFileExpectedAcl, implicit: false}}, shouldError: false},
			{name: "Setfacl of dir/..", ctx: setupContext(false, refFile, false, false, false, ""), aclTestPaths: []AclTestPath{{pathString: pkg.TestMount() + "/14dir/inner_dir/..", expectedAcl: refFileExpectedAcl, implicit: false}}, shouldError: false},

			{name: "Setfacl of dir with cleaning", ctx: setupContext(false, refFile6, false, false, false, ""), aclTestPaths: []AclTestPath{{pathString: pkg.TestMount() + "/15dir", expectedAcl: refFileExpectedAcl6, implicit: false}}, shouldError: false},
			{name: "Setfacl of dir incorrect", ctx: setupContext(false, refFile7, false, false, false, ""), aclTestPaths: []AclTestPath{{pathString: pkg.TestMount() + "/16dir", expectedAcl: refFileExpectedAcl, implicit: false}}, shouldError: true},
		},
		{
			{name: "Setfacl of file (skip)", ctx: setupContext(false, refFile, false, false, false, ""), aclTestPaths: []AclTestPath{{pathString: pkg.TestMount() + "/dir/no_facl.txt", expectedAcl: refFileExpectedAcl, implicit: false}}, shouldError: false, noChange: true},
			{name: "Setfacl of link to file (skip)", ctx: setupContext(false, refFile, false, false, false, ""), aclTestPaths: []AclTestPath{{pathString: pkg.TestMount() + "/dir/no_facl_link.txt", expectedAcl: refFileExpectedAcl, implicit: false}}, shouldError: false, noChange: true},
			{name: "Setfacl of link to dir (skip)", ctx: setupContext(false, refFile, false, false, false, ""), aclTestPaths: []AclTestPath{{pathString: pkg.TestMount() + "/dir/no_facl_link_dir", expectedAcl: refFileExpectedAcl, implicit: false}}, shouldError: false, noChange: true},
			{name: "Setfacl of .cvmfscatalog (error)", ctx: setupContext(false, refFile, false, false, false, ""), aclTestPaths: []AclTestPath{{pathString: pkg.TestMount() + "/12dir/.cvmfscatalog", expectedAcl: refFileExpectedAcl, implicit: false}}, shouldError: true, noChange: true},
		},
		{
			{name: "Mod facl of dir again (replace)", ctx: setupContext(false, refFile, true, false, false, fmt.Sprintf("g:%s:w", groupname)), aclTestPaths: []AclTestPath{{pathString: pkg.TestMount() + "/6dir", expectedAcl: modAclMask(addAclGroup(refFileExpectedAcl, fmt.Sprintf("group:%d:-w-", gid)), "mask::-w-"), implicit: false}}, shouldError: false},
		},
		{
			{name: "Mod facl of dir (user)", ctx: setupContext(false, refFile, true, false, false, "u::rx"), aclTestPaths: []AclTestPath{{pathString: pkg.TestMount() + "/17dir", expectedAcl: "user::r-x,group::r-x,other::r-x", implicit: false}}, shouldError: false},
			{name: "Mod facl of dir (group)", ctx: setupContext(false, refFile, true, false, false, "g::rwx"), aclTestPaths: []AclTestPath{{pathString: pkg.TestMount() + "/17dir", expectedAcl: "user::r-x,group::rwx,other::r-x", implicit: false}}, shouldError: false},
			{name: "Mod facl of dir (other)", ctx: setupContext(false, refFile, true, false, false, "o::rwx"), aclTestPaths: []AclTestPath{{pathString: pkg.TestMount() + "/17dir", expectedAcl: "user::r-x,group::rwx,other::rwx", implicit: false}}, shouldError: false},
			{name: "Mod facl of dir (multiple)", ctx: setupContext(false, refFile, true, false, false, "u::rwx,g::r-x,o::r-x"), aclTestPaths: []AclTestPath{{pathString: pkg.TestMount() + "/17dir", expectedAcl: "user::rwx,group::r-x,other::r-x", implicit: false}}, shouldError: false},
		},
	}
	if runTest {
		reinstateExternalFuncs := setupExternalFuncs()
		fastTestRunner(t, tests)
		reinstateExternalFuncs()
	}
	return tests, tearDown
}

func TestMain(m *testing.M) {
	confFile := pkg.SetupConfigFile()
	OVERRIDE_CONFIG_FLAG_SET = true
	OVERRIDE_CONFIG_PATH = confFile
	defer os.Remove(confFile)
	pkg.SetupEnvironmentE2E()
	code := m.Run()
	os.Exit(code)
}
