package main

import (
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	pathlib "github.com/chigopher/pathlib"
	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
)

type Test struct {
	name          string
	ctx           Context
	deleteStrings []string
	purgeList     []string
	shouldError   bool
}

func mockPurgeMaker(filesToPurge []string) func(ctx Context, db pkg.DB) error {
	return func(ctx Context, db pkg.DB) error {
		fmt.Println("Checking Purge")
		purgeSlice := db.QueryPurges()
		purgePathStrSlice := []string{}
		for _, purgeFile := range purgeSlice {
			purgePathStrSlice = append(purgePathStrSlice, purgeFile.PathStr)
		}
		sort.Strings(purgePathStrSlice)
		sort.Strings(filesToPurge)
		if len(purgeSlice) != len(filesToPurge) {
			err := fmt.Errorf("purging file lists are not equal, erroring")
			fmt.Println("Expected:")
			fmt.Println(filesToPurge)
			fmt.Println("Actual:")
			fmt.Println(purgeSlice)
			return err
		}
		for i, path := range purgePathStrSlice {
			if path != filesToPurge[i] {
				err := fmt.Errorf("path should not be purged: " + path)
				fmt.Println("Expected:")
				fmt.Println(filesToPurge)
				fmt.Println("Actual:")
				fmt.Println(purgeSlice)
				return err
			}
		}
		return nil
	}
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

func setupContext(recursive bool, purge bool) Context {
	ctx := Context{
		recursive:  recursive,
		purge:      purge,
		debug:      true,
		numWorkers: 8,
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

func setupExternalPurge(filesToPurge []string) func() {
	purgeHold := purge
	purge = mockPurgeMaker(filesToPurge)
	return func() {
		purge = purgeHold
	}
}

func TestTurboSpeed(t *testing.T) {
	t2 := E2ERmBasicHelper(t, false)
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
					var purgeTeardown func()
					if tc.purgeList != nil {
						purgeTeardown = setupExternalPurge(tc.purgeList)
					}
					paths := []*pathlib.Path{}
					for _, pathString := range tc.deleteStrings {
						paths = append(paths, pathlib.NewPath(pathString))
					}
					err := launchRm(tc.ctx, paths)
					if tc.shouldError {
						if err == nil {
							t.Fatalf("Test should have errored.")
						}
					} else {
						if err != nil {
							panic(err)
						}
					}
					if tc.purgeList != nil {
						purgeTeardown()
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
			var purgeTeardown func()
			if tc.purgeList != nil {
				purgeTeardown = setupExternalPurge(tc.purgeList)
			}
			paths := []*pathlib.Path{}
			for _, pathString := range tc.deleteStrings {
				paths = append(paths, pathlib.NewPath(pathString))
			}
			err := launchRm(tc.ctx, paths)
			if tc.shouldError {
				if err == nil {
					t.Fatalf("Test should have errored.")
				}
			} else {
				if err != nil {
					panic(err)
				}
			}
			if tc.purgeList != nil {
				purgeTeardown()
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

func TestE2ERmBasic(t *testing.T) {
	E2ERmBasicHelper(t, true)
}

func E2ERmBasicHelper(t *testing.T, runTest bool) [][]Test {
	var defaultContext = setupContext(false, false)
	var recursiveContext = setupContext(true, false)
	var purgeContext = setupContext(false, true)
	var recursivePurgeContext = setupContext(true, true)
	teardown, dotFileName := setupTestEnv()
	if runTest {
		defer teardown(t)
	}
	tests := [][]Test{
		{
			{name: "Rm repo root", ctx: defaultContext, deleteStrings: []string{pkg.TestMount()}, purgeList: nil, shouldError: true},
			{name: "Rm file", ctx: defaultContext, deleteStrings: []string{pkg.TestMount() + "/file.txt"}, purgeList: nil, shouldError: false},
			{name: "Rm dot file link", ctx: defaultContext, deleteStrings: []string{pkg.TestMount() + "/dot-file.txt"}, purgeList: nil, shouldError: false},
			{name: "Rm sym", ctx: defaultContext, deleteStrings: []string{pkg.TestMount() + "/sym_file.txt"}, purgeList: nil, shouldError: false},
			{name: "Rm broken sym", ctx: defaultContext, deleteStrings: []string{pkg.TestMount() + "/sym_file_broken.txt"}, purgeList: nil, shouldError: false},
			{name: "Rm dir no recursive", ctx: defaultContext, deleteStrings: []string{pkg.TestMount() + "/dir"}, purgeList: nil, shouldError: true},
			{name: "Rm empty dir no recursive", ctx: defaultContext, deleteStrings: []string{pkg.TestMount() + "/dir/inner_dir"}, purgeList: nil, shouldError: true},
			{name: "Rm file dir no recursive", ctx: defaultContext, deleteStrings: []string{pkg.TestMount() + "/dir/inner_dir", pkg.TestMount() + "/dir/file.txt"}, purgeList: nil, shouldError: true},
			{name: "Rm sym dir no recursive", ctx: defaultContext, deleteStrings: []string{pkg.TestMount() + "/dir/inner_dir", pkg.TestMount() + "/dir/sym_file.txt"}, purgeList: nil, shouldError: true},
			{name: "Rm file sym no recursive", ctx: defaultContext, deleteStrings: []string{pkg.TestMount() + "/dir/file.txt", pkg.TestMount() + "/dir/sym_file.txt"}, purgeList: nil, shouldError: false},
			{name: "Rm file purge", ctx: purgeContext, deleteStrings: []string{pkg.TestMount() + "/file2.txt"}, purgeList: []string{"file2.txt"}, shouldError: false},
			{name: "Rm . (no removal)", ctx: defaultContext, deleteStrings: []string{pkg.TestMount() + "/dir/inner_dir4/."}, purgeList: nil, shouldError: false},
			{name: "Rm .. (no removal)", ctx: defaultContext, deleteStrings: []string{pkg.TestMount() + "/dir/inner_dir4/.."}, purgeList: nil, shouldError: false},
			{name: "Rm .cvmfscatalog (no removal)", ctx: defaultContext, deleteStrings: []string{pkg.TestMount() + "/dir/inner_dir4/.cvmfscatalog"}, purgeList: nil, shouldError: false},
		},
		{
			{name: "Rm dot file", ctx: defaultContext, deleteStrings: []string{pkg.TestMount() + "/" + dotFileName}, purgeList: nil, shouldError: false},
			{name: "Rm empty dir recursive", ctx: recursiveContext, deleteStrings: []string{pkg.TestMount() + "/dir/inner_dir"}, purgeList: nil, shouldError: false},
			{name: "Rm dir recursive", ctx: recursiveContext, deleteStrings: []string{pkg.TestMount() + "/dir/inner_dir2"}, purgeList: nil, shouldError: false},
			{name: "Rm file no perms", ctx: defaultContext, deleteStrings: []string{pkg.TestMount() + "/check-permissions/file.txt"}, purgeList: nil, shouldError: true},
			{name: "Rm file no allow", ctx: defaultContext, deleteStrings: []string{pkg.TestMount() + "/disallow-delete/file.txt"}, purgeList: nil, shouldError: true},
		},
		{
			{name: "Rm dir purge all", ctx: recursivePurgeContext, deleteStrings: []string{pkg.TestMount() + "/dir"}, purgeList: []string{"dir/file2.txt", "dir/file3.txt", "dir/inner_dir3/file.txt"}, shouldError: false},
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
