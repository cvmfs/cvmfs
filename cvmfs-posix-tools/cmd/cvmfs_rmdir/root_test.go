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
	name          string
	ctx           Context
	deleteStrings []string
	shouldError   bool
}

func verifyDelete(ctx Context, deletePaths []string) bool {
	for _, deletePath := range deletePaths {
		deletePathName := pathlib.NewPath(deletePath).Name()
		if deletePathName == pkg.CurrentDirectory || deletePathName == pkg.PreviousDirectory || deletePathName == pkg.CVMFSProtectedFile || deletePathName == pkg.CVMFSAutoProtectedFile {
			_, err := os.Lstat(deletePath)
			if os.IsNotExist(err) {
				return false
			}
		} else {
			// Get relative, then parts and then for all but last add parent flag
			_, err := os.Lstat(deletePath)
			if !os.IsNotExist(err) {
				return false
			}
		}
	}
	return true
}

func setupContext() Context {
	ctx := Context{
		debug: true,
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
	t2 := E2ERmdirBasicHelper(t, false)
	maxTestCollectionLen := len(t2)
	testCollections := [][][]Test{t2}
	reinstateExternalFuncs := setupExternalFuncs()

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
					for _, pathString := range tc.deleteStrings {
						paths = append(paths, pathlib.NewPath(pathString))
					}
					err := launchRmdir(tc.ctx, paths)
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
						if !verifyDelete(tc.ctx, tc.deleteStrings) {
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
			for _, pathString := range tc.deleteStrings {
				paths = append(paths, pathlib.NewPath(pathString))
			}
			err := launchRmdir(tc.ctx, paths)
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
				if !verifyDelete(tc.ctx, tc.deleteStrings) {
					fmt.Println(tc.name + " failed")

					t.Fatalf("Filesystems not the same after rsync. Check for yourself.")
				}
			}
		}
	}
}

func TestE2ERmdirBasic(t *testing.T) {
	E2ERmdirBasicHelper(t, true)
}

func E2ERmdirBasicHelper(t *testing.T, runTest bool) [][]Test {
	var defaultContext = setupContext()
	teardown, _ := setupTestEnv()
	if runTest {
		defer teardown(t)
	}
	tests := [][]Test{
		{
			{name: "Rmdir repo root", ctx: defaultContext, deleteStrings: []string{pkg.TestMount()}, shouldError: true},
			{name: "Rmdir file fail", ctx: defaultContext, deleteStrings: []string{pkg.TestMount() + "/file.txt"}, shouldError: true},
			{name: "Rmdir dot file link fail", ctx: defaultContext, deleteStrings: []string{pkg.TestMount() + "/dot-file.txt"}, shouldError: true},
			{name: "Rmdir sym fail", ctx: defaultContext, deleteStrings: []string{pkg.TestMount() + "/sym_file.txt"}, shouldError: true},
			{name: "Rmdir full dir fail", ctx: defaultContext, deleteStrings: []string{pkg.TestMount() + "/dir"}, shouldError: true},
			{name: "Rmdir empty dir succeed", ctx: defaultContext, deleteStrings: []string{pkg.TestMount() + "/dir/inner_dir"}, shouldError: false},
			{name: "Rmdir multiple dir succeed", ctx: defaultContext, deleteStrings: []string{pkg.TestMount() + "/dir/inner_dir2", pkg.TestMount() + "/dir/inner_dir3"}, shouldError: false},
			{name: "Rmdir empty dir no perm fail", ctx: defaultContext, deleteStrings: []string{pkg.TestMount() + "/check-permissions/inner_dir"}, shouldError: true},
			{name: "Rmdir empty dir no allow fail", ctx: defaultContext, deleteStrings: []string{pkg.TestMount() + "/disallow-delete/inner_dir"}, shouldError: true},
			{name: "Rmdir . (no removal)", ctx: defaultContext, deleteStrings: []string{pkg.TestMount() + "/dir/inner_dir4/."}, shouldError: false},
			{name: "Rmdir .. (no removal)", ctx: defaultContext, deleteStrings: []string{pkg.TestMount() + "/dir/inner_dir4/.."}, shouldError: false},
			{name: "Rmdir .cvmfscatalog (no removal)", ctx: defaultContext, deleteStrings: []string{pkg.TestMount() + "/dir/inner_dir4/.cvmfscatalog"}, shouldError: false},
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
