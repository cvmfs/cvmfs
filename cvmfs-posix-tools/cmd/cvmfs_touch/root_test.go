package main

import (
	"fmt"
	"io/fs"
	"os"
	"testing"
	"time"

	pathlib "github.com/chigopher/pathlib"
	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
)

type TestPathPathInfo struct {
	pathString string // This may be equivalent to pathInfo.name, should verify
	pathInfo   fs.FileInfo
}

type Test struct {
	name          string
	ctx           Context
	expectedMtime time.Time
	paths         []TestPathPathInfo
	shouldError   bool
}

var TestTime = time.Date(2023, 9, 11, 14, 0, 0, 0, time.UTC)

func getDotschemifyFunction(contentAddressable bool) func(pathComponent string, fileHashData pkg.FileHashData) string {
	if contentAddressable {
		return func(pathComponent string, fileHashData pkg.FileHashData) string {
			return pathComponent
		}
	} else {
		return func(pathComponent string, fileHashData pkg.FileHashData) string {
			return fmt.Sprintf(".%s.%040x", pathComponent, fileHashData.Checksum)
		}
	}
}

func verifNoChanges(expectedPathInfo, pathInfo fs.FileInfo) bool {
	return expectedPathInfo.Size() == pathInfo.Size() && expectedPathInfo.Mode() == pathInfo.Mode()
}

func touchVerification(ctx Context, expectedPathInfo fs.FileInfo, expectedMtime time.Time, path *pathlib.Path) bool {
	var err error
	if !ctx.noDeref {
		path, err = path.ResolveAll()
		if err != nil {
			panic(err)
		}
	}
	pathInfo, err := os.Lstat(path.Clean().String())
	if err != nil {
		panic(err)
	}
	if !verifNoChanges(expectedPathInfo, pathInfo) {
		fmt.Println("Path obj changed unexpectedely in processing.")
		fmt.Println(path.String())
		fmt.Println(expectedPathInfo.Size())
		fmt.Println(pathInfo.Size())
		fmt.Println(expectedPathInfo.Mode())
		fmt.Println(pathInfo.Mode())
		return false
	}
	if !expectedMtime.Equal(pathInfo.ModTime()) {
		fmt.Println("Expected mtime and mtime are not the same")
		fmt.Println(path.String())
		fmt.Println(expectedMtime.String())
		fmt.Println(pathInfo.ModTime().String())
		return false
	}
	return true
}

func verifyTouch(ctx Context, expectedMtime time.Time, pathPathInfos []TestPathPathInfo) bool {
	for _, pathPathInfo := range pathPathInfos {
		path := pathlib.NewPath(pathPathInfo.pathString)
		if !touchVerification(ctx, pathPathInfo.pathInfo, expectedMtime, path) {
			return false
		}
	}
	return true
}

func setupContext(noDeref bool) Context {
	ctx := Context{
		noDeref: noDeref,
		debug:   true,
	}

	var err error
	ctx.cfg, _, _, _, err = pkg.GetCvmfsConfigurationInfo(
		pkg.TestMountName(),
		pathlib.NewPath(OVERRIDE_CONFIG_PATH),
	)
	if err != nil {
		panic(err)
	}

	return ctx
}

func setupExternalFuncs(hashData pkg.FileHashData) func() {
	graftHold := graft
	graft = pkg.Mock_graft_getter()
	destInCvmfsFromFilePathHold := destInCvmfsFromFilePath
	destInCvmfsFromFilePath = pkg.MockDestInCvmfsFromFilePath
	getRepoPathHold := getRepoPath
	getRepoPath = pkg.MockGetRepoPath
	timeNowHold := timeNow
	timeNow = func() time.Time {
		return TestTime
	}
	return func() {
		graft = graftHold
		destInCvmfsFromFilePath = destInCvmfsFromFilePathHold
		getRepoPath = getRepoPathHold
		timeNow = timeNowHold
	}
}

func TestTurboSpeed(t *testing.T) {
	maxTestCollectionLen := 0
	t1, td1, hashData := E2ETouchHelper(t, false)
	defer td1(t)
	testCollections := [][][]Test{t1}
	reinstateExternalFuncs := setupExternalFuncs(hashData)

	for _, testCollection := range testCollections {
		maxTestCollectionLen = max(len(testCollection), maxTestCollectionLen)
	}

	for i := 0; i < maxTestCollectionLen; i++ {
		megaGraftDb, err := pkg.NewCvmfsGraftingDB()
		if err != nil {
			panic(err)
		}
		graft = pkg.Mock_mega_graft(megaGraftDb)
		for _, testCollection := range testCollections {
			if len(testCollection) > i {
				for _, tc := range testCollection[i] {
					fmt.Printf("\nRunning test %s\n", tc.name)
					paths := []*pathlib.Path{}
					for _, pathPathInfo := range tc.paths {
						paths = append(paths, pathlib.NewPath(pathPathInfo.pathString))
					}
					err := launchTouch(tc.ctx, paths)
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
					if !tc.shouldError {
						if !verifyTouch(tc.ctx, tc.expectedMtime, tc.paths) {
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
			paths := []*pathlib.Path{}
			for _, pathPathInfo := range tc.paths {
				paths = append(paths, pathlib.NewPath(pathPathInfo.pathString))
			}
			err := launchTouch(tc.ctx, paths)
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
				if !verifyTouch(tc.ctx, tc.expectedMtime, tc.paths) {
					fmt.Println(tc.name + " failed")

					t.Fatalf("Some mode was not as it should be. Check test mount.")
				}
			}
		}
	}
}

func TestE2ETouch(t *testing.T) {
	E2ETouchHelper(t, true)
}

func E2ETouchHelper(t *testing.T, runTest bool) ([][]Test, func(t *testing.T), pkg.FileHashData) {
	var defaultContext = setupContext(false)
	var noDerefContext = setupContext(true)
	tearDown, _, fHashData := setupTestEnv(defaultContext)
	if runTest {
		defer tearDown(t)
	}
	dotschemifyPathComponent := getDotschemifyFunction(defaultContext.cfg.Repo.ContentAddressable)
	// t.Parallel()
	// setupEnvironmentE2E()
	fileInfo, err := os.Lstat(pkg.TestMount() + "/" + dotschemifyPathComponent("file.txt", fHashData))
	if err != nil {
		panic(err)
	}
	dirInfo, err := os.Lstat(pkg.TestMount() + "/dir")
	if err != nil {
		panic(err)
	}
	symFileInfo, err := os.Lstat(pkg.TestMount() + "/sym_file6.txt")
	if err != nil {
		panic(err)
	}
	symDirInfo, err := os.Lstat(pkg.TestMount() + "/sym_dir4")
	if err != nil {
		panic(err)
	}
	noOwnSymFileInfo, err := os.Lstat(pkg.TestMount() + "/no_own_sym_file4.txt")
	if err != nil {
		panic(err)
	}
	noOwnSymDirInfo, err := os.Lstat(pkg.TestMount() + "/no_own_sym_dir4")
	if err != nil {
		panic(err)
	}
	tests := [][]Test{
		{
			{name: "Non-existent file path", ctx: defaultContext, expectedMtime: TestTime, paths: []TestPathPathInfo{{pathString: pkg.TestMount() + "/nothing", pathInfo: nil}}, shouldError: true},
			{name: "Touching file path", ctx: defaultContext, expectedMtime: TestTime, paths: []TestPathPathInfo{{pathString: pkg.TestMount() + "/" + dotschemifyPathComponent("file.txt", fHashData), pathInfo: fileInfo}}, shouldError: false},
			{name: "Touching dir path", ctx: defaultContext, expectedMtime: TestTime, paths: []TestPathPathInfo{{pathString: pkg.TestMount() + "/dir", pathInfo: dirInfo}}, shouldError: false},
			{name: "Touching sym -> file path", ctx: defaultContext, expectedMtime: TestTime, paths: []TestPathPathInfo{{pathString: pkg.TestMount() + "/sym_file2.txt", pathInfo: fileInfo}}, shouldError: false},
			{name: "Touching sym -> dir path", ctx: defaultContext, expectedMtime: TestTime, paths: []TestPathPathInfo{{pathString: pkg.TestMount() + "/sym_dir2", pathInfo: dirInfo}}, shouldError: false},
			{name: "Touching multiple paths", ctx: defaultContext, expectedMtime: TestTime, paths: []TestPathPathInfo{{pathString: pkg.TestMount() + "/" + dotschemifyPathComponent("file3.txt", fHashData), pathInfo: fileInfo}, {pathString: pkg.TestMount() + "/" + dotschemifyPathComponent("file4.txt", fHashData), pathInfo: fileInfo}}, shouldError: false},

			{name: "Touching file path, no deref", ctx: noDerefContext, expectedMtime: TestTime, paths: []TestPathPathInfo{{pathString: pkg.TestMount() + "/" + dotschemifyPathComponent("file5.txt", fHashData), pathInfo: fileInfo}}, shouldError: false},
			{name: "Touching dir path, no deref", ctx: noDerefContext, expectedMtime: TestTime, paths: []TestPathPathInfo{{pathString: pkg.TestMount() + "/dir3", pathInfo: dirInfo}}, shouldError: false},
			{name: "Touching sym -> file path, no deref", ctx: noDerefContext, expectedMtime: TestTime, paths: []TestPathPathInfo{{pathString: pkg.TestMount() + "/sym_file6.txt", pathInfo: symFileInfo}}, shouldError: false},
			{name: "Touching sym -> dir path, no deref", ctx: noDerefContext, expectedMtime: TestTime, paths: []TestPathPathInfo{{pathString: pkg.TestMount() + "/sym_dir4", pathInfo: symDirInfo}}, shouldError: false},

			{name: "Touching no perm file (okay)", ctx: defaultContext, expectedMtime: TestTime, paths: []TestPathPathInfo{{pathString: pkg.TestMount() + "/" + dotschemifyPathComponent("no_perm.txt", fHashData), pathInfo: fileInfo}}, shouldError: false},
			{name: "Touching no perm dir (okay)", ctx: defaultContext, expectedMtime: TestTime, paths: []TestPathPathInfo{{pathString: pkg.TestMount() + "/no_own_dir", pathInfo: dirInfo}}, shouldError: false},
			{name: "Touching no perm file, no deref (okay)", ctx: noDerefContext, expectedMtime: TestTime, paths: []TestPathPathInfo{{pathString: pkg.TestMount() + "/" + dotschemifyPathComponent("no_perm2.txt", fHashData), pathInfo: fileInfo}}, shouldError: false},
			{name: "Touching no perm dir, no deref (okay)", ctx: noDerefContext, expectedMtime: TestTime, paths: []TestPathPathInfo{{pathString: pkg.TestMount() + "/no_own_dir2", pathInfo: dirInfo}}, shouldError: false},
			{name: "Touching no perm sym -> file (okay)", ctx: defaultContext, expectedMtime: TestTime, paths: []TestPathPathInfo{{pathString: pkg.TestMount() + "/no_own_sym_file3.txt", pathInfo: fileInfo}}, shouldError: false},
			{name: "Touching no perm sym -> dir (okay)", ctx: defaultContext, expectedMtime: TestTime, paths: []TestPathPathInfo{{pathString: pkg.TestMount() + "/no_own_sym_dir3", pathInfo: dirInfo}}, shouldError: false},
			{name: "Touching no perm sym -> file, no deref (okay)", ctx: noDerefContext, expectedMtime: TestTime, paths: []TestPathPathInfo{{pathString: pkg.TestMount() + "/no_own_sym_file4.txt", pathInfo: noOwnSymFileInfo}}, shouldError: false},
			{name: "Touching no perm sym -> dir, no deref (okay)", ctx: noDerefContext, expectedMtime: TestTime, paths: []TestPathPathInfo{{pathString: pkg.TestMount() + "/no_own_sym_dir4", pathInfo: noOwnSymDirInfo}}, shouldError: false},

			{name: "Touching no perm parent dir file, (error)", ctx: defaultContext, expectedMtime: TestTime, paths: []TestPathPathInfo{{pathString: pkg.TestMount() + "/check-permissions/no_own_dir/" + dotschemifyPathComponent("file.txt", fHashData), pathInfo: fileInfo}}, shouldError: true},
			{name: "Touching no perm parent dir dir, (error)", ctx: defaultContext, expectedMtime: TestTime, paths: []TestPathPathInfo{{pathString: pkg.TestMount() + "/check-permissions/no_own_dir/dir", pathInfo: dirInfo}}, shouldError: true},
			{name: "Touching no perm parent dir file, no deref, (error)", ctx: noDerefContext, expectedMtime: TestTime, paths: []TestPathPathInfo{{pathString: pkg.TestMount() + "/check-permissions/no_own_dir/" + dotschemifyPathComponent("file.txt", fHashData), pathInfo: fileInfo}}, shouldError: true},
			{name: "Touching no perm parent dir dir, no deref, (error)", ctx: noDerefContext, expectedMtime: TestTime, paths: []TestPathPathInfo{{pathString: pkg.TestMount() + "/check-permissions/no_own_dir/dir", pathInfo: dirInfo}}, shouldError: true},
			{name: "Touching no perm parent dir sym -> file, (error)", ctx: defaultContext, expectedMtime: TestTime, paths: []TestPathPathInfo{{pathString: pkg.TestMount() + "/check-permissions/no_own_dir/sym_file2.txt", pathInfo: fileInfo}}, shouldError: true},
			{name: "Touching no perm parent dir sym -> dir, (error)", ctx: defaultContext, expectedMtime: TestTime, paths: []TestPathPathInfo{{pathString: pkg.TestMount() + "/check-permissions/no_own_dir/sym_dir2", pathInfo: dirInfo}}, shouldError: true},
			{name: "Touching no perm parent dir sym -> file, no deref, (error)", ctx: noDerefContext, expectedMtime: TestTime, paths: []TestPathPathInfo{{pathString: pkg.TestMount() + "/check-permissions/no_own_dir/sym_file2.txt", pathInfo: symFileInfo}}, shouldError: true},
			{name: "Touching no perm parent dir sym -> dir, no deref, (error)", ctx: noDerefContext, expectedMtime: TestTime, paths: []TestPathPathInfo{{pathString: pkg.TestMount() + "/check-permissions/no_own_dir/sym_dir2", pathInfo: symDirInfo}}, shouldError: true},
		},
	}
	if runTest {
		reinstateExternalFuncs := setupExternalFuncs(fHashData)
		fastTestRunner(t, tests)
		reinstateExternalFuncs()
	}
	return tests, tearDown, fHashData
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
