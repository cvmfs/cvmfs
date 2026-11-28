package main

import (
	"fmt"
	"os"
	"testing"
	"time"

	pathlib "github.com/chigopher/pathlib"
	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
)

type Test struct {
	name            string
	ctx             Context
	targetStrings   []string
	destPathString  string
	destIsDir       bool
	shouldError     bool
	overridePathStr string
}

func compareLinksTest(targetStr string, destPath *pathlib.Path) bool {
	destTarget, err := os.Readlink(destPath.Clean().String())
	if err != nil {
		panic(err)
	}

	return destTarget == pathlib.NewPath(targetStr).Clean().String()
}

func verifyCopy(ctx Context, targetStrings []string, destPathString string, noDest, destIsDir bool) bool {
	// In this function every file will be verified
	// Not only that, it should also ensure that the mode, owner, group, and acls have all been copied as well as expected.
	// That may be some extra hardware, but it's a test and it makes sense
	// For checking that stuff, can just do it based on config, or if -a, pull it from the source file itself
	if noDest {
		if len(targetStrings) > 1 {
			return false
		}
		destPath := pathlib.NewPath(pkg.CurrentDirectory)
		targetStringName := pathlib.NewPath(targetStrings[0]).Name()
		return compareLinksTest(targetStrings[0], destPath.Join(targetStringName))
	} else {
		destPath := pathlib.NewPath(destPathString)
		if len(targetStrings) == 1 {
			if destIsDir {
				targetName := pathlib.NewPath(targetStrings[0]).Name()
				destPath = destPath.Join(targetName)
			}
			return compareLinksTest(targetStrings[0], destPath)
		} else {
			for _, targetString := range targetStrings {
				targetPath := pathlib.NewPath(targetString)
				if !compareLinksTest(targetString, destPath.Join(targetPath.Name())) {
					return false
				}
			}
			return true
		}
	}
}

func setupContext(symbolic, force, noDest, noDeref bool) Context {
	ctx := Context{
		symbolic: symbolic,
		force:    force,
		debug:    true,
		noDest:   noDest,
		noDeref:  noDeref,
	}

	var err error
	ctx.cfg, _, ctx.uid, ctx.groupIdMap, err = pkg.GetCvmfsConfigurationInfo(
		pkg.TestMountName(),
		pathlib.NewPath(OVERRIDE_CONFIG_PATH),
	)
	if err != nil {
		panic(err)
	}

	return ctx
}

// var forceContext = setupContext(false, true, false)

func setupExternalFuncs() func() {
	graftHold := graft
	graft = pkg.Mock_graft_getter()
	destInCvmfsHold := destInCvmfs
	destInCvmfs = func(destPath *pathlib.Path) (bool, error) { return true, nil }
	getRepoPathHold := getRepoPath
	getRepoPath = pkg.MockGetRepoPath
	return func() {
		graft = graftHold
		destInCvmfs = destInCvmfsHold
		getRepoPath = getRepoPathHold
	}
}

func TestTurboSpeed(t *testing.T) {
	tearDown := setupTestEnv()
	defer tearDown(t)
	t1 := E2ESymBasicHelper(t, false)
	testCollections := [][][]Test{t1}
	reinstateExternalFuncs := setupExternalFuncs()

	originalDir, err := pkg.GetAbsolutePath(pathlib.NewPath(pkg.CurrentDirectory))
	if err != nil {
		panic(err)
	}
	originalDirString := originalDir.Clean().String()
	for i := 0; i < 3; i++ {
		if i == 2 {
			if err := os.Chdir(pkg.TestMount() + "/dir"); err != nil {
				panic(err)
			}
		}
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
					fmt.Printf("\nRunning test %s\n", tc.name)
					err := launchLn(tc.ctx, tc.targetStrings, tc.destPathString)
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
				}
			}
		}
		if i == 2 {
			if err := os.Chdir(originalDirString); err != nil {
				panic(err)
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
		if i == 2 {
			if err := os.Chdir(pkg.TestMount() + "/dir"); err != nil {
				panic(err)
			}
		}
		fmt.Println("Verifying")
		for _, testCollection := range testCollections {
			if len(testCollection) > i {
				for _, tc := range testCollection[i] {
					if !tc.shouldError {
						if !verifyCopy(tc.ctx, tc.targetStrings, tc.destPathString, tc.ctx.noDest, tc.destIsDir) {
							t.Fatalf("Filesystems not the same after rsync. Check for yourself.")
						}
					}
				}
			}
		}
		if i == 2 {
			if err := os.Chdir(originalDirString); err != nil {
				panic(err)
			}
		}
	}
	reinstateExternalFuncs()
}

func fastTestRunner(t *testing.T, tests [][]Test) {
	originalDir, err := pkg.GetAbsolutePath(pathlib.NewPath(pkg.CurrentDirectory))
	if err != nil {
		panic(err)
	}
	originalDirString := originalDir.Clean().String()
	for i, tList := range tests {
		if i == 2 {
			if err := os.Chdir(pkg.TestMount() + "/dir"); err != nil {
				panic(err)
			}
		}
		for _, tc := range tList {
			fmt.Printf("\nRunning test %s\n", tc.name)
			err := launchLn(tc.ctx, tc.targetStrings, tc.destPathString)
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
		if i == 2 {
			if err := os.Chdir(originalDirString); err != nil {
				panic(err)
			}
		}
		fmt.Println("Unmount Repo")
		pkg.UmountRepo()
		time.Sleep(500 * time.Millisecond)
		fmt.Println("Mount Repo")
		pkg.MountRepo()
		if i == 2 {
			if err := os.Chdir(pkg.TestMount() + "/dir"); err != nil {
				panic(err)
			}
		}
		fmt.Println("Verifying")
		for _, tc := range tList {
			if !tc.shouldError {
				if !verifyCopy(tc.ctx, tc.targetStrings, tc.destPathString, tc.ctx.noDest, tc.destIsDir) {
					fmt.Println(tc.name + " failed")

					t.Fatalf("Filesystems not the same after rsync. Check for yourself.")
				}
			}
		}
		if i == 2 {
			if err := os.Chdir(originalDirString); err != nil {
				panic(err)
			}
		}
	}
}

func TestE2ESymBasic(t *testing.T) {
	tearDown := setupTestEnv()
	E2ESymBasicHelper(t, true)
	tearDown(t)
}

func E2ESymBasicHelper(t *testing.T, runTest bool) [][]Test {
	var defaultContext = setupContext(false, false, false, false)
	var symbolicContext = setupContext(true, false, false, false)
	var forceSymbolicContext = setupContext(true, true, false, false)
	var symbolicNoDestContext = setupContext(true, false, true, false)
	var forceSymbolicNoDestContext = setupContext(true, true, true, false)
	var symbolicNoDerefContext = setupContext(true, false, false, true)
	var forceSymbolicNoDerefContext = setupContext(true, true, false, true)
	tests := [][]Test{
		{
			{name: "Link without symbolic flag", ctx: defaultContext, targetStrings: []string{"oliver_path"}, destPathString: pkg.TestMount() + "/sym_fail", shouldError: true, destIsDir: false},
			{name: "Basic link target", ctx: symbolicContext, targetStrings: []string{"oliver_path"}, destPathString: pkg.TestMount() + "/symlink1", shouldError: false, destIsDir: false},
			{name: "Basic link over file", ctx: symbolicContext, targetStrings: []string{"oliver_path"}, destPathString: pkg.TestMount() + "/file.txt", shouldError: true, destIsDir: false},
			{name: "Basic link over file force", ctx: forceSymbolicContext, targetStrings: []string{"oliver_path"}, destPathString: pkg.TestMount() + "/file.txt", shouldError: false, destIsDir: false},
			{name: "Basic link into dir", ctx: symbolicContext, targetStrings: []string{"oliver_path"}, destPathString: pkg.TestMount() + "/dir", shouldError: false, destIsDir: true},
			{name: "Link of dir", ctx: symbolicContext, targetStrings: []string{"dir"}, destPathString: pkg.TestMount() + "/sym_dir", shouldError: false, destIsDir: false},
			{name: "Basic link over sym", ctx: symbolicContext, targetStrings: []string{"oliver_path"}, destPathString: pkg.TestMount() + "/sym_file2.txt", shouldError: true, destIsDir: false},
			{name: "Basic link over sym force", ctx: forceSymbolicContext, targetStrings: []string{"oliver_path"}, destPathString: pkg.TestMount() + "/sym_file2.txt", shouldError: false, destIsDir: false},
			{name: "Links into ghost dest", ctx: symbolicContext, targetStrings: []string{"o_path", "l_path"}, destPathString: pkg.TestMount() + "/ghost_dest", shouldError: false, destIsDir: true},
			{name: "Links into file", ctx: symbolicContext, targetStrings: []string{"o_path", "l_path"}, destPathString: pkg.TestMount() + "/file2.txt", shouldError: true, destIsDir: false},
			{name: "Links into file force", ctx: forceSymbolicContext, targetStrings: []string{"o_path", "l_path"}, destPathString: pkg.TestMount() + "/file2.txt", shouldError: true, destIsDir: false},
			{name: "Links into sym of file", ctx: symbolicContext, targetStrings: []string{"o_path", "l_path"}, destPathString: pkg.TestMount() + "/dir/sym_file.txt", shouldError: true, destIsDir: false},
			{name: "Links into sym of file force", ctx: forceSymbolicContext, targetStrings: []string{"o_path", "l_path"}, destPathString: pkg.TestMount() + "/dir/sym_file.txt", shouldError: true, destIsDir: false},
			{name: "Link over file impl", ctx: symbolicContext, targetStrings: []string{"file.txt"}, destPathString: pkg.TestMount() + "/dir", shouldError: true, destIsDir: true},
			{name: "Link over file impl force", ctx: forceSymbolicContext, targetStrings: []string{"file.txt"}, destPathString: pkg.TestMount() + "/dir", shouldError: false, destIsDir: true},
			{name: "Links over file dir impl", ctx: symbolicContext, targetStrings: []string{"file2.txt", "inner_dir"}, destPathString: pkg.TestMount() + "/dir", shouldError: true, destIsDir: true},
			{name: "Links over file dir impl force", ctx: forceSymbolicContext, targetStrings: []string{"file2.txt", "inner_dir"}, destPathString: pkg.TestMount() + "/dir", shouldError: true, destIsDir: true},
			{name: "Links over sym dir impl", ctx: symbolicContext, targetStrings: []string{"sym_file2.txt", "inner_dir"}, destPathString: pkg.TestMount() + "/dir", shouldError: true, destIsDir: true},
			{name: "Links over sym dir impl force", ctx: forceSymbolicContext, targetStrings: []string{"sym_file2.txt", "inner_dir"}, destPathString: pkg.TestMount() + "/dir", shouldError: true, destIsDir: true},
			{name: "Links over sym file impl", ctx: symbolicContext, targetStrings: []string{"sym_file2.txt", "file2.txt"}, destPathString: pkg.TestMount() + "/dir", shouldError: true, destIsDir: true},
			{name: "Links over sym file impl force", ctx: forceSymbolicContext, targetStrings: []string{"sym_file2.txt", "file2.txt"}, destPathString: pkg.TestMount() + "/dir", shouldError: false, destIsDir: true},
			{name: "Link into no perm dir", ctx: forceSymbolicContext, targetStrings: []string{"sym_path_gg"}, destPathString: pkg.TestMount() + "/check-permissions", shouldError: true, destIsDir: true},
			{name: "Link into no perm dir", ctx: forceSymbolicContext, targetStrings: []string{"sym_path_gg"}, destPathString: pkg.TestMount() + "/check-permissions2", shouldError: false, destIsDir: true},
			{name: "Link over disallow-delete file", ctx: forceSymbolicContext, targetStrings: []string{"sym_path_again"}, destPathString: pkg.TestMount() + "/disallow-delete/file.txt", shouldError: true, destIsDir: true},
			{name: "Link into group perm dir", ctx: symbolicContext, targetStrings: []string{"sym_path_gg"}, destPathString: pkg.TestMount() + "/check-permissions3", shouldError: false, destIsDir: true},
			{name: "Symlink with /. path", ctx: symbolicContext, targetStrings: []string{"dot_file.txt"}, destPathString: pkg.TestMount() + "/.", shouldError: false, destIsDir: true},
			{name: "Symlink with /./dest path", ctx: symbolicContext, targetStrings: []string{"o_path"}, destPathString: pkg.TestMount() + "/./dot_link", shouldError: false, destIsDir: false},
			{name: "Symlink with /.. path", ctx: symbolicContext, targetStrings: []string{"dot_dot_file.txt"}, destPathString: pkg.TestMount() + "/dir/..", shouldError: false, destIsDir: true},
			{name: "Symlink with /../dest path", ctx: symbolicContext, targetStrings: []string{"o_path"}, destPathString: pkg.TestMount() + "/dir/../dot_dot_link", shouldError: false, destIsDir: false},
			{name: "Symlink over /.cvmfscatalog path (error)", ctx: symbolicContext, targetStrings: []string{"a_link"}, destPathString: pkg.TestMount() + "/dir/.cvmfscatalog", shouldError: true, destIsDir: false},
			{name: "Symlink force over /.cvmfscatalog path (error)", ctx: forceSymbolicContext, targetStrings: []string{"a_link"}, destPathString: pkg.TestMount() + "/dir/.cvmfscatalog", shouldError: true, destIsDir: false},
			{name: "Symlink to .", ctx: symbolicContext, targetStrings: []string{"."}, destPathString: pkg.TestMount() + "/link_to_dot", shouldError: false, destIsDir: false},
			{name: "Symlink to ..", ctx: symbolicContext, targetStrings: []string{".."}, destPathString: pkg.TestMount() + "/link_to_dot_dot", shouldError: false, destIsDir: false},
			{name: "Symlink to .cvmfscatalog", ctx: symbolicContext, targetStrings: []string{".cvmfscatalog"}, destPathString: pkg.TestMount() + "/link_to_cvmfscatalog", shouldError: false, destIsDir: false},
			{name: "Symlink to inner .", ctx: symbolicContext, targetStrings: []string{"dir/./inner_dir"}, destPathString: pkg.TestMount() + "/link_to_inner_dot", shouldError: false, destIsDir: false},
			{name: "Symlink to inner ..", ctx: symbolicContext, targetStrings: []string{"dir/../dir"}, destPathString: pkg.TestMount() + "/link_to_inner_dot_dot", shouldError: false, destIsDir: false},

			{name: "Basic link target, no deref", ctx: symbolicNoDerefContext, targetStrings: []string{"oliver_path"}, destPathString: pkg.TestMount() + "/symlink1ND", shouldError: false, destIsDir: false},
			{name: "Basic link over file, no deref", ctx: symbolicNoDerefContext, targetStrings: []string{"oliver_path"}, destPathString: pkg.TestMount() + "/fileND.txt", shouldError: true, destIsDir: false},
			{name: "Basic link over file force, no deref", ctx: forceSymbolicNoDerefContext, targetStrings: []string{"oliver_path"}, destPathString: pkg.TestMount() + "/fileND.txt", shouldError: false, destIsDir: false},
			{name: "Basic link over sym, no deref", ctx: symbolicNoDerefContext, targetStrings: []string{"oliver_path"}, destPathString: pkg.TestMount() + "/sym_file2ND.txt", shouldError: true, destIsDir: false},
			{name: "Basic link over sym force, no deref", ctx: forceSymbolicNoDerefContext, targetStrings: []string{"oliver_path"}, destPathString: pkg.TestMount() + "/sym_file2ND.txt", shouldError: false, destIsDir: false},
			{name: "Link of dir", ctx: symbolicContext, targetStrings: []string{"dir"}, destPathString: pkg.TestMount() + "/sym_dirND", shouldError: false, destIsDir: false},
		},
		{
			{name: "Basic link over same", ctx: symbolicContext, targetStrings: []string{"garb_path"}, destPathString: pkg.TestMount() + "/symlink1", shouldError: true, destIsDir: false},
			{name: "Basic link over same force", ctx: forceSymbolicContext, targetStrings: []string{"new_path"}, destPathString: pkg.TestMount() + "/symlink1", shouldError: false, destIsDir: false},
			{name: "Links into existing sym dir", ctx: symbolicContext, targetStrings: []string{"e_path", "r_path"}, destPathString: pkg.TestMount() + "/sym_dir", shouldError: false, destIsDir: true},
			{name: "Links into existing dir", ctx: symbolicContext, targetStrings: []string{"i_path", "v_path"}, destPathString: pkg.TestMount() + "/ghost_dest", shouldError: false, destIsDir: true},
			{name: "Link over sym impl", ctx: symbolicContext, targetStrings: []string{"sym_file.txt"}, destPathString: pkg.TestMount() + "/dir", shouldError: true, destIsDir: true},
			{name: "Link over sym impl force", ctx: forceSymbolicContext, targetStrings: []string{"sym_file.txt"}, destPathString: pkg.TestMount() + "/dir", shouldError: false, destIsDir: true},

			{name: "Links over existing sym dir, no deref", ctx: symbolicNoDerefContext, targetStrings: []string{"oliver_path"}, destPathString: pkg.TestMount() + "/sym_dirND", shouldError: true, destIsDir: false},
			{name: "Links over existing sym dir force, no deref", ctx: forceSymbolicNoDerefContext, targetStrings: []string{"oliver_path"}, destPathString: pkg.TestMount() + "/sym_dirND", shouldError: false, destIsDir: false},
		},
		{
			{name: "Basic link no dest", ctx: symbolicNoDestContext, targetStrings: []string{"o2_path"}, destPathString: "", shouldError: false, destIsDir: true},
			{name: "Basic link over file no dest", ctx: symbolicNoDestContext, targetStrings: []string{"file3.txt"}, destPathString: "", shouldError: true, destIsDir: true},
			{name: "Basic link over file no dest force", ctx: forceSymbolicNoDestContext, targetStrings: []string{"file3.txt"}, destPathString: "", shouldError: true, destIsDir: true},
			{name: "Basic link over dir no dest", ctx: symbolicNoDestContext, targetStrings: []string{"file3.txt"}, destPathString: "", shouldError: true, destIsDir: true},
			{name: "Basic link over dir no dest force", ctx: forceSymbolicNoDestContext, targetStrings: []string{"file3.txt"}, destPathString: "", shouldError: true, destIsDir: true},
			{name: "Basic link over sym no dest", ctx: symbolicNoDestContext, targetStrings: []string{"sym_file3.txt"}, destPathString: "", shouldError: true, destIsDir: true},
			{name: "Basic link over sym no dest force", ctx: forceSymbolicNoDestContext, targetStrings: []string{"sym_file3.txt"}, destPathString: "", shouldError: false, destIsDir: true},
		},
	}
	if runTest {
		reinstateExternalFuncs := setupExternalFuncs()
		fastTestRunner(t, tests)
		reinstateExternalFuncs()
	}
	return tests
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
