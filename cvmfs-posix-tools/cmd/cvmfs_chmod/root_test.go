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

type Test struct {
	name         string
	ctx          Context
	expectedMode int
	modeStr      string
	paths        []string
	shouldError  bool
}

func compareModes(expectedMode int, pathInfo fs.FileInfo) bool {
	return uint16(expectedMode) == uint16(pathInfo.Mode())
}

func recursiveChmodVerification(ctx Context, expectedMode int, path *pathlib.Path) bool {
	pathInfo, err := os.Lstat(path.Clean().String())
	if err != nil {
		panic(err)
	}
	if !pathlib.IsSymlink(pathInfo.Mode()) {
		if !compareModes(expectedMode, pathInfo) {
			fmt.Printf("%s: not in expectedMode %o\n", path, expectedMode)
			return false
		}
		if !pkg.VerifyModeMatchACL(path) {
			fmt.Printf("%s: unexpected ACL\n", path)
			return false
		}
		if ctx.recursive && pathlib.IsDir(pathInfo.Mode()) {
			childPaths, err := pkg.ReadDirExclude(ctx.cfg, path, false)
			if err != nil {
				panic(err)
			}
			for _, childPath := range childPaths {
				if childPath.Name() != pkg.CVMFSProtectedFile && childPath.Name() != pkg.CVMFSAutoProtectedFile {
					if !recursiveChmodVerification(ctx, expectedMode, childPath) {
						return false
					}
				}
			}
		}
	} else {
		if ctx.cfg.Repo.DotScheme {
			isDot, err := pkg.IsDotSchemeLink(pathlib.NewPath("/").JoinPath(path))
			if err != nil {
				panic(err)
			}
			if isDot {
				pathInfo, err := os.Stat(path.Clean().String())
				if err != nil {
					panic(err)
				}
				if !compareModes(expectedMode, pathInfo) {
					fmt.Printf("%s: not in expectedMode %o\n", path, expectedMode)
					return false
				}
			}
		}
	}
	fmt.Printf("%s: OK\n", path)
	return true
}

func verifyChmod(ctx Context, expectedMode int, pathStrings []string) bool {
	for _, pathString := range pathStrings {
		path := pathlib.NewPath(pathString)
		if !recursiveChmodVerification(ctx, expectedMode, path) {
			return false
		}
	}
	return true
}

func setupContext(recursive bool, referenceStr string) Context {
	ctx := Context{
		recursive:    recursive,
		reference:    referenceStr,
		referenceSet: referenceStr != "",
		debug:        true,
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

func setupExternalFuncs(hashData pkg.FileHashData) func() {
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
	t1, td1, hashData := E2EChmodHelper(t, false)
	defer td1(t)
	testCollections := [][][]Test{t1}
	reinstateExternalFuncs := setupExternalFuncs(hashData)

	for i := 0; i < 3; i++ {
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
					for _, pathString := range tc.paths {
						paths = append(paths, pathlib.NewPath(pathString))
					}
					err := launchChmod(tc.ctx, tc.modeStr, paths)
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
						if !verifyChmod(tc.ctx, tc.expectedMode, tc.paths) {
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
			for _, pathString := range tc.paths {
				paths = append(paths, pathlib.NewPath(pathString))
			}
			err := launchChmod(tc.ctx, tc.modeStr, paths)
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
				if !verifyChmod(tc.ctx, tc.expectedMode, tc.paths) {
					fmt.Println(tc.name + " failed")

					t.Fatalf("Some mode was not as it should be. Check test mount.")
				}
			}
		}
	}
}

func TestE2EChmod(t *testing.T) {
	E2EChmodHelper(t, true)
}

func E2EChmodHelper(t *testing.T, runTest bool) ([][]Test, func(t *testing.T), pkg.FileHashData) {
	var defaultContext = setupContext(false, "")
	var recursiveContext = setupContext(true, "")
	tearDown, refFile, fHashData := setupTestEnv(defaultContext)
	if runTest {
		defer tearDown(t)
	}
	// t.Parallel()
	// setupEnvironmentE2E()
	tests := [][]Test{
		{
			{name: "Chmod of file, malformed", ctx: defaultContext, expectedMode: int(0o0754), modeStr: "A garbage mode", paths: []string{pkg.TestMount() + "/file.txt"}, shouldError: true},
			{name: "Chmod of file, no own", ctx: defaultContext, expectedMode: int(0o0754), modeStr: "0754", paths: []string{pkg.TestMount() + "/check-permissions/no_perm.txt"}, shouldError: true},
			{name: "Chmod of dir, no own", ctx: defaultContext, expectedMode: int(0o0754), modeStr: "0754", paths: []string{pkg.TestMount() + "/check-permissions/no_own_dir"}, shouldError: true},

			{name: "Chmod of file, no own, no perm check", ctx: defaultContext, expectedMode: int(0o0754), modeStr: "0754", paths: []string{pkg.TestMount() + "/no_perm.txt"}, shouldError: false},
			{name: "Chmod of dir, no own, no perm check", ctx: defaultContext, expectedMode: int(0o0754), modeStr: "0754", paths: []string{pkg.TestMount() + "/no_own_dir"}, shouldError: false},

			{name: "Chmod of file, octal", ctx: defaultContext, expectedMode: int(0o0754), modeStr: "0754", paths: []string{pkg.TestMount() + "/file.txt"}, shouldError: false},
			{name: "Chmod of dir, octal", ctx: defaultContext, expectedMode: int(0o0775), modeStr: "0775", paths: []string{pkg.TestMount() + "/dir"}, shouldError: false},
			{name: "Chmod of sym, octal (nothing)", ctx: defaultContext, expectedMode: int(0o0), modeStr: "0755", paths: []string{pkg.TestMount() + "/sym_file.txt"}, shouldError: false},
			{name: "Chmod of file, ugo format", ctx: defaultContext, expectedMode: int(0o0644), modeStr: "ugo-x", paths: []string{pkg.TestMount() + "/file2.txt"}, shouldError: false},
			{name: "Chmod of file, ugo format but mutliple", ctx: defaultContext, expectedMode: int(0o0654), modeStr: "ugo-x,g+x", paths: []string{pkg.TestMount() + "/file3.txt"}, shouldError: false},
			{name: "Chmod of multiple files", ctx: defaultContext, expectedMode: int(0o0640), modeStr: "0640", paths: []string{pkg.TestMount() + "/dir/file.txt", pkg.TestMount() + "/dir/file2.txt"}, shouldError: false},
			{name: "Chmod of ACL dir", ctx: defaultContext, expectedMode: int(0o0777), modeStr: "0777", paths: []string{pkg.TestMount() + "/acldir"}, shouldError: false},

			// FAIL: SHOULD MAYBE NOT DO ANYTHING, OTHERWISE GOOD
			// {name: "Chmod of file/, (error)", ctx: defaultContext, expectedMode: int(0o0754), modeStr: "0754", paths: []string{pkg.TestMount()+ "/dir/inner_dir3/file.txt/"}, shouldError: true},
			// {name: "Chmod of sym/, (error)", ctx: defaultContext, expectedMode: int(0o0754), modeStr: "0754", paths: []string{pkg.TestMount()+ "/dir/inner_dir3/sym_file.txt/"}, shouldError: true},

			// FAIL: SHOULD NOT DO ANYTHING OR CHANGE FILE/SYM
			// {name: "Chmod of file/., (error)", ctx: defaultContext, expectedMode: int(0o0754), modeStr: "0754", paths: []string{pkg.TestMount()+ "/dir/inner_dir3/file.txt/."}, shouldError: true},
			// {name: "Chmod of sym/., (error)", ctx: defaultContext, expectedMode: int(0o0754), modeStr: "0754", paths: []string{pkg.TestMount()+ "/dir/inner_dir3/sym_file.txt/."}, shouldError: true},

			// FAIL: SHOULD NOT DO ANYTHING OR CHANGE LEVEL UP
			// {name: "Chmod of file/.., (error)", ctx: defaultContext, expectedMode: int(0o0754), modeStr: "0754", paths: []string{pkg.TestMount()+ "/dir/inner_dir3/file.txt/.."}, shouldError: true},
			// {name: "Chmod of sym/.., (error)", ctx: defaultContext, expectedMode: int(0o0754), modeStr: "0754", paths: []string{pkg.TestMount()+ "/dir/inner_dir3/sym_file.txt/.."}, shouldError: true},

			{name: "Chmod of dir/, change dir", ctx: defaultContext, expectedMode: int(0o0754), modeStr: "0754", paths: []string{pkg.TestMount() + "/dir/inner_dir3/inner_dir/"}, shouldError: false},

			{name: "Chmod of dir/., change dir", ctx: defaultContext, expectedMode: int(0o0754), modeStr: "0754", paths: []string{pkg.TestMount() + "/dir/inner_dir3.1/inner_dir/."}, shouldError: false},
			{name: "Chmod of dir/.., change dir", ctx: defaultContext, expectedMode: int(0o0754), modeStr: "0754", paths: []string{pkg.TestMount() + "/dir/inner_dir3.2/inner_dir/.."}, shouldError: false},
		},
		{
			{name: "Chmod of file, reference", ctx: setupContext(false, refFile), expectedMode: int(0o0600), modeStr: "", paths: []string{pkg.TestMount() + "/file.txt"}, shouldError: false},
			{name: "Chmod of dir recursive", ctx: recursiveContext, expectedMode: int(0o0775), modeStr: "0775", paths: []string{pkg.TestMount() + "/dir"}, shouldError: false},
		},
		{
			{name: "Chmod of cvmfs catalog, (error)", ctx: defaultContext, expectedMode: int(0o0754), modeStr: "0754", paths: []string{pkg.TestMount() + "/dir/.cvmfscatalog"}, shouldError: true},

			{name: "Chmod of dir recursive /.", ctx: recursiveContext, expectedMode: int(0o0754), modeStr: "0754", paths: []string{pkg.TestMount() + "/dir/inner_dir4/."}, shouldError: false},

			{name: "Chmod of dir recursive /..", ctx: recursiveContext, expectedMode: int(0o0754), modeStr: "0754", paths: []string{pkg.TestMount() + "/dir/inner_dir5/inner_dir/.."}, shouldError: false},
			{name: "Chmod of dir recursive /", ctx: recursiveContext, expectedMode: int(0o0754), modeStr: "0754", paths: []string{pkg.TestMount() + "/dir/inner_dir6/"}, shouldError: false},
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
