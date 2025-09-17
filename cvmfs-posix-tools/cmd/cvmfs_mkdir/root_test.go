package main

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/go-acl"
	pathlib "github.com/chigopher/pathlib"
	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
	"github.com/rs/zerolog/log"
	mode "github.com/tonistiigi/dchapes-mode"
)

type Test struct {
	name           string
	ctx            Context
	dirPathStrings []DirRealPath
	shouldError    bool
}

type DirRealPath struct {
	dirPath   string
	realPath  string
	ghostPath string
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

func verifySetfacl(ctx Context, dirPath *pathlib.Path) bool {
	aclString, err := pkg.GetAclFromFile(ctx.faclFile)
	if err != nil {
		panic(err)
	}

	return compareAcls(aclString, dirPath)
}

func compareDirsTest(ctx Context, dirPath *pathlib.Path, parentDir bool) bool {
	dirInfo, err := os.Lstat(dirPath.Clean().String())
	if err != nil {
		panic(err)
	}
	fmt.Println(dirPath.Clean().String())
	dirOwner, dirGroup, dirMode, err := pkg.GetPathPerms(dirInfo)
	if err != nil {
		panic(err)
	}

	expectedDirOwner, expectedDirGroup, _, expectedDirMode := pkg.PermsForGroup(ctx.cfg)
	if !parentDir {
		if ctx.modeSet {
			changeSet, err := mode.Parse(ctx.mode)
			if err != nil {
				panic(err)
			}
			expectedDirMode = int(changeSet.Apply(os.FileMode(expectedDirMode)))
		}
	}
	if uint16(dirMode) != uint16(expectedDirMode) || expectedDirOwner != dirOwner || expectedDirGroup != dirGroup {
		fmt.Println("dirs not equal")
		fmt.Println(uint16(dirMode))
		fmt.Println(uint16(expectedDirMode))
		fmt.Println(dirOwner)
		fmt.Println(expectedDirOwner)
		fmt.Println(dirGroup)
		fmt.Println(expectedDirGroup)
		return false
	}
	if ctx.faclFile != "" {
		if !verifySetfacl(ctx, dirPath) {
			fmt.Println(dirPath.String())
			fmt.Println("Facl file:")
			fmt.Println(ctx.faclFile)
			return false
		}
	}

	return true
}

func verifyCopy(ctx Context, dirRealPaths []DirRealPath) bool {
	for _, dirRealPath := range dirRealPaths {
		// Get relative, then parts and then for all but last add parent flag
		ghostParts := pathlib.NewPath(dirRealPath.ghostPath).Clean().Parts()
		compareDir := pathlib.NewPath(dirRealPath.realPath)
		for idx, ghostPart := range ghostParts {
			compareDir = compareDir.Join(ghostPart)
			parentDir := idx != len(ghostParts)-1
			if !compareDirsTest(ctx, compareDir, parentDir) {
				return false
			}
		}
	}
	return true
}

func setupContext(mode string, parent bool, faclFile string) Context {
	ctx := Context{
		modeSet:  mode != "",
		mode:     mode,
		parent:   parent,
		debug:    true,
		faclFile: faclFile,
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
	destInCvmfsHold := destInCvmfs
	destInCvmfs = func(destPath *pathlib.Path) (bool, error) { return true, nil }
	getRepoPathHold := pkg.GetRepoPath
	getRepoPath = pkg.MockGetRepoPath
	return func() {
		graft = graftHold
		destInCvmfs = destInCvmfsHold
		getRepoPath = getRepoPathHold
	}
}

func TestTurboSpeed(t *testing.T) {
	t2, td2 := E2EDirBasicHelper(t, false)
	defer td2(t)
	testCollections := [][][]Test{t2}
	reinstateExternalFuncs := setupExternalFuncs()

	for i := 0; i < 2; i++ {
		// var wg sync.WaitGroup
		// var wg2 sync.WaitGroup
		// errs := make(chan error, 100)
		// errs2 := make(chan error, 100)
		megaGraftDb, err := pkg.NewCvmfsGraftingDB()
		if err != nil {
			panic(err)
		}
		graft = pkg.Mock_mega_graft(megaGraftDb)
		for _, testCollection := range testCollections {
			if len(testCollection) > i {
				for _, tc := range testCollection[i] {
					// wg.Add(1)
					// go func(tc Test){
					// 	defer wg.Done()
					fmt.Printf("\nRunning test %s\n", tc.name)
					// if tc.noChange {
					// 	graft = mock_no_graft
					// }
					dirStrings := []*pathlib.Path{}
					for _, dirRel := range tc.dirPathStrings {
						dirStrings = append(dirStrings, pathlib.NewPath(dirRel.dirPath))
					}
					err := launchMkdir(tc.ctx, dirStrings)
					if tc.shouldError {
						if err == nil {
							t.Fatalf("Test should have errored.")
							// errs <- fmt.Errorf("Test should have errored.")
						}
					} else {
						if err != nil {
							panic(err)
							// errs <- err
						}
					}
					// if tc.noChange {
					// 	graft = mock_mega_graft(megaGraftDb)
					// }
					// }(tc)
				}
			}
		}
		// wg.Wait()
		// close(errs)
		// if len(errs) > 0 {
		// 	for err := range errs {
		// 		t.Fatalf(err.Error())
		// 	}
		// }
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
					// wg2.Add(1)
					// go func(tc Test){
					// 	defer wg2.Done()
					if !tc.shouldError {
						if !verifyCopy(tc.ctx, tc.dirPathStrings) {
							t.Fatalf("Filesystems not the same after rsync. Check for yourself.")
							// errs2 <- fmt.Errorf("Filesystems not the same after rsync. Check for yourself.")
						}
					}
					// }(tc)
				}
			}
		}
		// wg2.Wait()
		// close(errs2)
		// if len(errs2) > 0 {
		// 	for err := range errs2 {
		// 		t.Fatalf(err.Error())
		// 	}
		// }
	}
	reinstateExternalFuncs()
}

func fastTestRunner(t *testing.T, tests [][]Test) {
	for _, tList := range tests {
		for _, tc := range tList {
			fmt.Printf("\nRunning test %s\n", tc.name)
			dirPaths := []*pathlib.Path{}
			for _, dirRel := range tc.dirPathStrings {
				dirPaths = append(dirPaths, pathlib.NewPath(dirRel.dirPath))
			}
			err := launchMkdir(tc.ctx, dirPaths)
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
		fmt.Println("Unmount Repo")
		pkg.UmountRepo()
		time.Sleep(500 * time.Millisecond)
		fmt.Println("Mount Repo")
		pkg.MountRepo()
		fmt.Println("Verifying")
		for _, tc := range tList {
			if !tc.shouldError {
				if !verifyCopy(tc.ctx, tc.dirPathStrings) {
					fmt.Println(tc.name + " failed")

					t.Fatalf("Filesystems not the same after rsync. Check for yourself.")
				}
			}
		}
	}
}

func TestE2EDirBasic(t *testing.T) {
	E2EDirBasicHelper(t, true)
}

func E2EDirBasicHelper(t *testing.T, runTest bool) ([][]Test, func(t *testing.T)) {
	addPermissionedDirs()

	tearDown, faclFile := setupTestEnv()
	if runTest {
		defer tearDown(t)
	}

	tests := [][]Test{
		{
			{name: "Create new dir", ctx: setupContext("", false, ""), dirPathStrings: []DirRealPath{{dirPath: pkg.TestMount() + "/dir1", realPath: pkg.TestMount(), ghostPath: "dir1"}}, shouldError: false},
			{name: "Create parent dirs without -p fails", ctx: setupContext("", false, ""), dirPathStrings: []DirRealPath{{dirPath: pkg.TestMount() + "/dir2/inner/inner2", realPath: pkg.TestMount(), ghostPath: "dir2/inner/inner2"}}, shouldError: true},
			{name: "Create parent dirs with -p succeeds", ctx: setupContext("", true, ""), dirPathStrings: []DirRealPath{{dirPath: pkg.TestMount() + "/dir2/inner/inner2", realPath: pkg.TestMount(), ghostPath: "dir2/inner/inner2"}}, shouldError: false},
			{name: "Create with mode no p fails", ctx: setupContext("0777", false, ""), dirPathStrings: []DirRealPath{{dirPath: pkg.TestMount() + "/dir3/inner/inner2", realPath: pkg.TestMount(), ghostPath: "dir3/inner/inner2"}}, shouldError: true},
			{name: "Create with mode and p succeeds, proper modes", ctx: setupContext("0777", true, ""), dirPathStrings: []DirRealPath{{dirPath: pkg.TestMount() + "/dir3/inner/inner2", realPath: pkg.TestMount(), ghostPath: "dir3/inner/inner2"}}, shouldError: false},
			{name: "Proper modes on only one dir", ctx: setupContext("0777", false, ""), dirPathStrings: []DirRealPath{{dirPath: pkg.TestMount() + "/dir4", realPath: pkg.TestMount(), ghostPath: "dir4"}}, shouldError: false},
			{name: "Create with mode no p fails go syntax", ctx: setupContext("go-x", false, ""), dirPathStrings: []DirRealPath{{dirPath: pkg.TestMount() + "/dir5/inner/inner2", realPath: pkg.TestMount(), ghostPath: "dir5/inner/inner2"}}, shouldError: true},
			{name: "Create with mode and p succeeds, proper modes go syntax", ctx: setupContext("go-x", true, ""), dirPathStrings: []DirRealPath{{dirPath: pkg.TestMount() + "/dir5/inner/inner2", realPath: pkg.TestMount(), ghostPath: "dir5/inner/inner2"}}, shouldError: false},
			{name: "Proper modes on only one dir go syntax", ctx: setupContext("go-x", false, ""), dirPathStrings: []DirRealPath{{dirPath: pkg.TestMount() + "/dir6", realPath: pkg.TestMount(), ghostPath: "dir6"}}, shouldError: false},
			{name: "Poorly formatted errors fail", ctx: setupContext("aBadFormat", false, ""), dirPathStrings: []DirRealPath{{dirPath: pkg.TestMount() + "/dir6.5/inner/inner2", realPath: pkg.TestMount(), ghostPath: "dir6.5/inner/inner2"}}, shouldError: true},

			{name: "Create multiple new dir", ctx: setupContext("", false, ""), dirPathStrings: []DirRealPath{{dirPath: pkg.TestMount() + "/dir7", realPath: pkg.TestMount(), ghostPath: "dir7"}, {dirPath: pkg.TestMount() + "/dir8", realPath: pkg.TestMount(), ghostPath: "dir8"}}, shouldError: false},
			{name: "Create multiple new dir fails no p", ctx: setupContext("", false, ""), dirPathStrings: []DirRealPath{{dirPath: pkg.TestMount() + "/dir9/inner/inner2", realPath: pkg.TestMount(), ghostPath: "dir9/inner/inner2"}, {dirPath: pkg.TestMount() + "/dir10/inner/inner2", realPath: pkg.TestMount(), ghostPath: "dir10/inner/inner2"}}, shouldError: true},
			{name: "Create succeeds with p", ctx: setupContext("", true, ""), dirPathStrings: []DirRealPath{{dirPath: pkg.TestMount() + "/dir9/inner/inner2", realPath: pkg.TestMount(), ghostPath: "dir9/inner/inner2"}, {dirPath: pkg.TestMount() + "/dir10/inner/inner2", realPath: pkg.TestMount(), ghostPath: "dir10/inner/inner2"}}, shouldError: false},
			{name: "Create with mode and p succeeds, proper modes", ctx: setupContext("0777", true, ""), dirPathStrings: []DirRealPath{{dirPath: pkg.TestMount() + "/dir11/inner/inner2", realPath: pkg.TestMount(), ghostPath: "dir11/inner/inner2"}, {dirPath: pkg.TestMount() + "/dir12/inner/inner2", realPath: pkg.TestMount(), ghostPath: "dir12/inner/inner2"}}, shouldError: false},
			{name: "Create with mode and p succeeds, proper modes, go stuff", ctx: setupContext("o+w", true, ""), dirPathStrings: []DirRealPath{{dirPath: pkg.TestMount() + "/dir13/inner/inner2", realPath: pkg.TestMount(), ghostPath: "dir13/inner/inner2"}, {dirPath: pkg.TestMount() + "/dir14/inner/inner2", realPath: pkg.TestMount(), ghostPath: "dir14/inner/inner2"}}, shouldError: false},
			{name: "Disallow upload prevents dir upload", ctx: setupContext("", false, ""), dirPathStrings: []DirRealPath{{dirPath: pkg.TestMount() + "/disallow-upload/no_dir", realPath: pkg.TestMount(), ghostPath: "no_dir"}}, shouldError: true},
			{name: "Perms prevent dir upload", ctx: setupContext("", false, ""), dirPathStrings: []DirRealPath{{dirPath: pkg.TestMount() + "/check-permissions/no_dir", realPath: pkg.TestMount(), ghostPath: "no_dir"}}, shouldError: true},
			{name: "Setup acl dir", ctx: setupContext("", false, ""), dirPathStrings: []DirRealPath{{dirPath: pkg.TestMount() + "/no-acl", realPath: pkg.TestMount(), ghostPath: "no-acl"}}, shouldError: false},
			{name: "Set acl of dir", ctx: setupContext("", false, faclFile), dirPathStrings: []DirRealPath{{dirPath: pkg.TestMount() + "/acl", realPath: pkg.TestMount(), ghostPath: "acl"}}, shouldError: false},
		},
		{
			{name: "Create dir /. (error)", ctx: setupContext("", false, ""), dirPathStrings: []DirRealPath{{dirPath: pkg.TestMount() + "/dir2/.", realPath: pkg.TestMount() + "/dir2/.", ghostPath: ""}}, shouldError: true},
			{name: "Create dir /.. (error)", ctx: setupContext("", false, ""), dirPathStrings: []DirRealPath{{dirPath: pkg.TestMount() + "/dir2/inner/..", realPath: pkg.TestMount() + "/dir2/inner/..", ghostPath: ""}}, shouldError: true},
			{name: "Create dir /.cvmfscatalog (error)", ctx: setupContext("", false, ""), dirPathStrings: []DirRealPath{{dirPath: pkg.TestMount() + "/dir2/.cvmfscatalog", realPath: pkg.TestMount() + "/dir2/.cvmfscatalog", ghostPath: ""}}, shouldError: true},
			{name: "Create same dir (error)", ctx: setupContext("", false, ""), dirPathStrings: []DirRealPath{{dirPath: pkg.TestMount() + "/dir1", realPath: pkg.TestMount() + "/dir1", ghostPath: ""}}, shouldError: true},
			{name: "Create same dir p", ctx: setupContext("", true, ""), dirPathStrings: []DirRealPath{{dirPath: pkg.TestMount() + "/dir1", realPath: pkg.TestMount() + "/dir1", ghostPath: ""}}, shouldError: false},
			{name: "Create same dir chain (error)", ctx: setupContext("", false, ""), dirPathStrings: []DirRealPath{{dirPath: pkg.TestMount() + "/dir2/inner/inner2", realPath: pkg.TestMount() + "/dir2/inner/inner2", ghostPath: ""}}, shouldError: true},
			{name: "Create same dir chain p", ctx: setupContext("", true, ""), dirPathStrings: []DirRealPath{{dirPath: pkg.TestMount() + "/dir2/inner/inner2", realPath: pkg.TestMount() + "/dir2/inner/inner2", ghostPath: ""}}, shouldError: false},
			{name: "Create new dir into non-dir", ctx: setupContext("", false, ""), dirPathStrings: []DirRealPath{{dirPath: pkg.TestMount() + "/sym_file.txt/dir", realPath: pkg.TestMount(), ghostPath: "dir"}}, shouldError: true},
			{name: "Create new dir into non-dir part 2", ctx: setupContext("", false, ""), dirPathStrings: []DirRealPath{{dirPath: pkg.TestMount() + "/file.txt/dir", realPath: pkg.TestMount(), ghostPath: "dir"}}, shouldError: true},
			{name: "Create new dir into non-dir part 3", ctx: setupContext("", false, ""), dirPathStrings: []DirRealPath{{dirPath: pkg.TestMount() + "/sym_file2.txt/dir", realPath: pkg.TestMount(), ghostPath: "dir"}}, shouldError: true},
			{name: "Set acl of dir, no perms", ctx: setupContext("", false, faclFile), dirPathStrings: []DirRealPath{{dirPath: pkg.TestMount() + "no-acl/acl", realPath: pkg.TestMount() + "/no-acl", ghostPath: "no-acl/acl"}}, shouldError: true},
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
