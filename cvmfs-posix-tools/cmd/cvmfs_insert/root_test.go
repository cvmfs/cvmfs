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
	name               string
	ctx                Context
	pathPairs          []SrcDest
	shouldError        bool
	noErrorNoDotscheme bool
	useFacl            bool
}

var (
	defaultContext,
	aclContext,
	noAclContext Context
)

func setupContexts() {
	defaultContext = setupContext(false)
	aclContext = func() Context { newCtx := defaultContext; newCtx.acls = pkg.ACLPreserveAll; return newCtx }()
	noAclContext = func() Context { newCtx := defaultContext; newCtx.acls = pkg.ACLNone; return newCtx }()
}

// Conveniece contexts for debugging if necessary
// var dryrunContext = setupContext(true)

// The indexes for when tests are run are very important.
// i == 2 is for when that test would result in an error in grafting
// i == 3 Is for testing the default permissions
func TestTurboSpeed(t *testing.T) {
	maxTestCollectionLen := 0
	testsToRun := []func(*testing.T, bool) ([][]Test, CreateLists, func(t *testing.T)){
		E2EFileBasicHelper,
		E2EDirBasicHelper,
		E2ESymBasicHelper,
		E2EPermissionCheckingHelper,
		E2EPermissionDefaultCheckingHelper,
		E2EFaclBasicHelper,
	}
	testCollections := [][][]Test{}
	fullCreateLists := EmptyCreateLists
	for _, test := range testsToRun {
		testCollection, createLists, teardown := test(t, false)
		defer teardown(t)
		testCollections = append(testCollections, testCollection)
		maxTestCollectionLen = max(len(testCollection), maxTestCollectionLen)
		fullCreateLists.aclDirs = append(fullCreateLists.aclDirs, createLists.aclDirs...)
		fullCreateLists.dirs = append(fullCreateLists.dirs, createLists.dirs...)
		fullCreateLists.files = append(fullCreateLists.files, createLists.files...)
		fullCreateLists.links = append(fullCreateLists.links, createLists.links...)
	}
	// maxTestCollectionLen = 2
	reinstateExternalFuncs := setupExternalFuncs()
	addObjects(defaultContext, fullCreateLists)
	for i := 0; i < maxTestCollectionLen; i++ {
		megaGraftDb, err := pkg.NewCvmfsGraftingDB()
		if err != nil {
			panic(err)
		}
		graft = pkg.Mock_mega_graft_options(megaGraftDb)
		var getCvmfsConfInfoHold func(repoName string, cvmfsConfFile *pathlib.Path) (pkg.ConfStruct, string, int, map[int]bool, error)
		if i == 3 { // 3 is reserved for giving no default permissions
			getCvmfsConfInfoHold = getCvmfsConfInfo
			getCvmfsConfInfo = getCvmfsConfInfoDefaultMock
		}
		if i == 2 { // 2 is reserved for erroring grafts
			graft = pkg.Mock_graft_getter_options()
		}
		// Run rsync for this set of tests
		for _, testCollection := range testCollections {
			if len(testCollection) > i {
				for _, tc := range testCollection[i] {
					fmt.Printf("\nRunning test %s\n", tc.name)
					err := LaunchInsert(tc.ctx, pkg.TestMount(), genInsertCsv(tc.pathPairs, tc.useFacl))
					if tc.shouldError && (tc.ctx.cfg.Repo.DotScheme || !tc.noErrorNoDotscheme) {
						if err == nil {
							t.Fatalf("Test should have errored.")
						}
					} else if err != nil {
						panic(err)
					}
				}
			}
		}
		if i == 3 { // 3 is reserved for giving no default permissions
			getCvmfsConfInfo = getCvmfsConfInfoHold
		}
		if i == 2 { // 2 is reserved for erroring grafts
			graft = pkg.Mock_mega_graft_options(megaGraftDb)
		}
		pkg.Mock_graft_getter()(megaGraftDb, "", "low", true)
		if err := megaGraftDb.Teardown(true); err != nil {
			panic(err)
		}
		fmt.Println("Unmount Repo")
		pkg.UmountRepo()
		time.Sleep(500 * time.Millisecond)
		fmt.Println("Mount Repo")
		pkg.MountRepo()
		fmt.Println("Verifying")
		// Verify the previously ran rsync runs with the file system
		for _, testCollection := range testCollections {
			if len(testCollection) > i {
				for _, tc := range testCollection[i] {
					if !(tc.shouldError && (tc.ctx.cfg.Repo.DotScheme || !tc.noErrorNoDotscheme)) {
						if !verifyInsert(tc.ctx, tc.pathPairs, pkg.TestMount(), tc.useFacl) {
							fmt.Printf("\nTest %s Failed\n", tc.name)
							t.Fatalf("Filesystems not the same after rsync. Check for yourself.")
						}
					}
				}
			}
		}
	}
	reinstateExternalFuncs()
}

// Something to be aware of, this runs each graft individually whereas the turbo runs each set
// simultaneously. May result in weird states if one isn't careful designing tests.
func testRunner(t *testing.T, tests [][]Test) {
	for _, tList := range tests {
		for _, tc := range tList {
			fmt.Printf("\nRunning test %s\n", tc.name)
			// if tc.noChange {
			// 	graft = pkg.Mock_no_graft_options
			// }
			// if tc.shouldError {
			// 	graft = pkg.GraftWithOptions
			// }
			err := LaunchInsert(tc.ctx, pkg.TestMount(), genInsertCsv(tc.pathPairs, tc.useFacl))
			if tc.shouldError && (tc.ctx.cfg.Repo.DotScheme || !tc.noErrorNoDotscheme) {
				if err == nil {
					t.Fatalf("Test should have errored.")
				}
			} else {
				if err != nil {
					panic(err)
				}
			}
			// if tc.shouldError {
			// 	graft = pkg.Mock_graft_getter_options()
			// }
			// if tc.noChange {
			// 	graft = pkg.Mock_graft_getter_options()
			// }
		}
		fmt.Println("Unmount Repo")
		pkg.UmountRepo()
		time.Sleep(500 * time.Millisecond)
		fmt.Println("Mount Repo")
		pkg.MountRepo()
		fmt.Println("Verifying")
		for _, tc := range tList {
			if !(tc.shouldError && (tc.ctx.cfg.Repo.DotScheme || !tc.noErrorNoDotscheme)) {
				if !verifyInsert(tc.ctx, tc.pathPairs, pkg.TestMount(), tc.useFacl) {
					fmt.Println(tc.name + " failed")

					t.Fatalf("Filesystems not the same after rsync. Check for yourself.")
				}
			}
		}
	}
}

func TestE2EFileBasic(t *testing.T) {
	E2EFileBasicHelper(t, true)
}

// ADD THE OBJECTS CREATED AS OUTPUT OF FUNCTION
func E2EFileBasicHelper(t *testing.T, runTest bool) ([][]Test, CreateLists, func(t *testing.T)) {
	// t.Parallel()
	// setupEnvironmentE2E()
	// TODO: Can consolidate this add objects and put it at the beginning of the code. Currently that extra few seconds isn't too bad.
	t1teardownFileE2E, srcName, updateName, fDotName, fDotDotName, fCatalog, fInner := setupFileTestsE2E(t)
	if runTest {
		defer t1teardownFileE2E(t)
	}
	fCatalog = fCatalog
	tests := [][]Test{
		{
			{name: "file to new path", ctx: defaultContext, pathPairs: []SrcDest{{srcName, "f1.txt"}}, shouldError: false},
			{name: "file to ghost (error)", ctx: defaultContext, pathPairs: []SrcDest{{srcName, "ghost_dir/test_file.txt"}}, shouldError: true},
			{name: "multiple files to new path", ctx: defaultContext, pathPairs: []SrcDest{{srcName, "f2.txt"}, {srcName, "f3.txt"}}, shouldError: false},
			{name: "multiple files to same path", ctx: defaultContext, pathPairs: []SrcDest{{srcName, "f4.txt"}, {srcName, "f4.txt"}}, shouldError: true},
			{name: "file with trailing slash (no error)", ctx: defaultContext, pathPairs: []SrcDest{{srcName + "/", "f5.txt"}}, shouldError: false},
			{name: "file. to new path", ctx: defaultContext, pathPairs: []SrcDest{{fDotName, "f6.txt."}}, shouldError: false},
			{name: "file.. to new path", ctx: defaultContext, pathPairs: []SrcDest{{fDotDotName, "f7.txt.."}}, shouldError: false},

			{name: "file trailing /.", ctx: defaultContext, pathPairs: []SrcDest{{srcName + "/.", "f8.txt"}}, shouldError: false},
			{name: "file trailing /..", ctx: defaultContext, pathPairs: []SrcDest{{fInner + "/..", "f9.txt_dir"}}, shouldError: false},

			{name: "file over sym to file", ctx: defaultContext, pathPairs: []SrcDest{{srcName, "fileSetupSymFile"}}, shouldError: false},
			{name: "file over sym to dir", ctx: defaultContext, pathPairs: []SrcDest{{srcName, "fileSetupSymDir"}}, shouldError: false},
			{name: "file over broken sym", ctx: defaultContext, pathPairs: []SrcDest{{srcName, "fileSetupSymBroken"}}, shouldError: false},
			{name: "Refuse .cvmfscatalog upload (error)", ctx: defaultContext, pathPairs: []SrcDest{{srcName, ".cvmfscatalog"}}, shouldError: true},
			{name: "file to dot", ctx: defaultContext, pathPairs: []SrcDest{{srcName, "."}}, shouldError: true},
			{name: "file to dot dot", ctx: defaultContext, pathPairs: []SrcDest{{srcName, ".."}}, shouldError: true},
			{name: "file to path/dot", ctx: defaultContext, pathPairs: []SrcDest{{srcName, "fileSetupFullDir/."}}, shouldError: true},
			{name: "file to path/dot dot", ctx: defaultContext, pathPairs: []SrcDest{{srcName, "fileSetupFullDir/.."}}, shouldError: true},
		},
		{
			{name: "file over same", ctx: defaultContext, pathPairs: []SrcDest{{srcName, "f1.txt"}}, shouldError: false},
			{name: "file update", ctx: defaultContext, pathPairs: []SrcDest{{updateName, "f2.txt"}}, shouldError: false},
		},
		{
			{name: "file over empty dir", ctx: defaultContext, pathPairs: []SrcDest{{srcName, "fileSetupEmptyDir"}}, shouldError: true, noErrorNoDotscheme: true},
			{name: "file over full dir", ctx: defaultContext, pathPairs: []SrcDest{{srcName, "fileSetupFullDir"}}, shouldError: true, noErrorNoDotscheme: true},
		},
	}
	if runTest {
		reinstateExternalFuncs := setupExternalFuncs()
		addObjects(defaultContext, TestFileObjects)
		testRunner(t, tests)
		reinstateExternalFuncs()
	}
	return tests, TestFileObjects, t1teardownFileE2E
}

func TestE2EDirBasic(t *testing.T) {
	E2EDirBasicHelper(t, true)
}

func E2EDirBasicHelper(t *testing.T, runTest bool) ([][]Test, CreateLists, func(t *testing.T)) {
	// t.Parallel()
	// setupEnvironmentE2E()
	teardownFileDirE2E, fName, emptyDir, dotDir, dotDotDir, catalogDir, innerDir := setupDirTestE2E(t)
	if runTest {
		defer teardownFileDirE2E(t)
	}
	catalogDir = catalogDir
	tests := [][]Test{
		{
			{name: "dir non-recursive (error)", ctx: defaultContext, pathPairs: []SrcDest{{emptyDir, "d1"}}, shouldError: false},
			{name: "dir into ghost dir (error)", ctx: defaultContext, pathPairs: []SrcDest{{emptyDir, "ghost_dir/d1"}}, shouldError: true},
			{name: "multiple dirs to new path", ctx: defaultContext, pathPairs: []SrcDest{{emptyDir, "d2"}, {emptyDir, "d3"}}, shouldError: false},
			{name: "multiple dirs to same path (okay)", ctx: defaultContext, pathPairs: []SrcDest{{emptyDir, "d4"}, {emptyDir, "d4"}}, shouldError: false},
			{name: "dir to empty trailing dot", ctx: defaultContext, pathPairs: []SrcDest{{dotDir, "d5."}}, shouldError: false},
			{name: "dir to empty trailing dot", ctx: defaultContext, pathPairs: []SrcDest{{dotDotDir, "d6.."}}, shouldError: false},
			{name: "empty dir/ copies in", ctx: defaultContext, pathPairs: []SrcDest{{emptyDir + "/", "d7"}}, shouldError: false},
			{name: "empty dir/. copies in", ctx: defaultContext, pathPairs: []SrcDest{{emptyDir + "/.", "d8"}}, shouldError: false},
			{name: "empty dir/.. copies in", ctx: defaultContext, pathPairs: []SrcDest{{innerDir + "/..", "d9"}}, shouldError: false},
			{name: "dir then inner dir, file", ctx: defaultContext, pathPairs: []SrcDest{{emptyDir, "d10"}, {emptyDir, "d10/d1"}, {fName, "d10/f1.txt"}}, shouldError: false},
			{name: "inner dir, then dir", ctx: defaultContext, pathPairs: []SrcDest{{emptyDir, "d11/d1"}, {emptyDir, "d11"}}, shouldError: true},
			{name: "inner file, then dir", ctx: defaultContext, pathPairs: []SrcDest{{fName, "d12/f1.txt"}, {emptyDir, "d12"}}, shouldError: true},
			{name: "Refuse .cvmfscatalog dir upload (error)", ctx: defaultContext, pathPairs: []SrcDest{{catalogDir, ".cvmfscatalog"}}, shouldError: true},
			{name: "dir to dot", ctx: defaultContext, pathPairs: []SrcDest{{emptyDir, "."}}, shouldError: true},
			{name: "dir to dot dot", ctx: defaultContext, pathPairs: []SrcDest{{emptyDir, ".."}}, shouldError: true},
			{name: "dir to path/dot", ctx: defaultContext, pathPairs: []SrcDest{{emptyDir, "dirSetupFullDir/."}}, shouldError: true},
			{name: "dir to path/dot dot", ctx: defaultContext, pathPairs: []SrcDest{{emptyDir, "dirSetupFullDir/.."}}, shouldError: true},
		},
		{
			{name: "empty dir over same", ctx: defaultContext, pathPairs: []SrcDest{{emptyDir, "d1"}}, shouldError: false},
			{name: "empty dir over full", ctx: defaultContext, pathPairs: []SrcDest{{emptyDir, "d10"}}, shouldError: false},
		},
		{
			{name: "dir over sym to file", ctx: defaultContext, pathPairs: []SrcDest{{emptyDir, "dirSetupSymFile"}}, shouldError: true},
			{name: "dir over sym to dir", ctx: defaultContext, pathPairs: []SrcDest{{emptyDir, "dirSetupSymDir"}}, shouldError: true},
			{name: "dir over broken sym", ctx: defaultContext, pathPairs: []SrcDest{{emptyDir, "dirSetupSymBroken"}}, shouldError: true},
			{name: "dir over file", ctx: defaultContext, pathPairs: []SrcDest{{emptyDir, "dirSetupFile.txt"}}, shouldError: true},
		},
	}
	if runTest {
		reinstateExternalFuncs := setupExternalFuncs()
		addObjects(defaultContext, TestDirObjects)
		testRunner(t, tests)
		reinstateExternalFuncs()
	}
	return tests, TestDirObjects, teardownFileDirE2E
}

func TestE2ESymBasic(t *testing.T) {
	E2ESymBasicHelper(t, true)
}

func E2ESymBasicHelper(t *testing.T, runTest bool) ([][]Test, CreateLists, func(t *testing.T)) {
	// t.Parallel()
	// setupEnvironmentE2E()
	teardownFileE2E, _, _, symDir, symFile, _, _, brokenSymFile, _, trailingDot, trailingDotDot, trailingDotDir, trailingDotDotDir, cvmfsCatalog, innerSymFile, innerSymDir := setupSymTestE2E(t)
	if runTest {
		defer teardownFileE2E(t)
	}
	cvmfsCatalog = cvmfsCatalog
	tests := [][]Test{
		{
			// {name: "file and dir to empty", ctx: recursiveContext, srcPathStrings: []string{fName, emptyDir}, destPathString: pkg.TestMount(), shouldError: false, successNoDotScheme: true},
			{name: "broken sym upload", ctx: defaultContext, pathPairs: []SrcDest{{brokenSymFile, "sf1"}}, shouldError: true},
			{name: "sym of dir into ghost dir (error)", ctx: defaultContext, pathPairs: []SrcDest{{symFile, "ghost_dir/sd1"}}, shouldError: true},
			{name: "sym of file into ghost dir (error)", ctx: defaultContext, pathPairs: []SrcDest{{symDir, "ghost_dir2/sf1"}}, shouldError: true},
			{name: "sym of dir to empty", ctx: defaultContext, pathPairs: []SrcDest{{symDir, "sd1"}}, shouldError: false},
			{name: "sym of file to empty", ctx: defaultContext, pathPairs: []SrcDest{{symFile, "sf2"}}, shouldError: false},
			{name: "sym of dir, then sym of inner dir, inner file", ctx: defaultContext, pathPairs: []SrcDest{{symDir, "sd2"}, {symDir, "sd2/sd1"}, {symFile, "sd2/sf1"}}, shouldError: false},
			{name: "sym of inner dir, the sym of dir", ctx: defaultContext, pathPairs: []SrcDest{{symDir, "sd3/sd1"}, {symDir, "sd3"}}, shouldError: true},
			{name: "sym of inner file, the sym of dir", ctx: defaultContext, pathPairs: []SrcDest{{symFile, "sd4/sf1"}, {symDir, "sd4"}}, shouldError: true},
			{name: "sym of dir multiple", ctx: defaultContext, pathPairs: []SrcDest{{symDir, "sd5"}, {symDir, "sd6"}}, shouldError: false},
			{name: "sym of file multiple", ctx: defaultContext, pathPairs: []SrcDest{{symFile, "sf3"}, {symFile, "sf4"}}, shouldError: false},
			{name: "sym of dir multiple same", ctx: defaultContext, pathPairs: []SrcDest{{symDir, "sd7"}, {symDir, "sd7"}}, shouldError: false},
			{name: "sym of file multiple same", ctx: defaultContext, pathPairs: []SrcDest{{symFile, "sf5"}, {symFile, "sf5"}}, shouldError: true},
			{name: "sym of file trailing dot", ctx: defaultContext, pathPairs: []SrcDest{{trailingDot, "sf6."}}, shouldError: false},
			{name: "sym of file trailing dot dot", ctx: defaultContext, pathPairs: []SrcDest{{trailingDotDot, "sf7.."}}, shouldError: false},
			{name: "sym of dir trailing dot", ctx: defaultContext, pathPairs: []SrcDest{{trailingDotDir, "sd8."}}, shouldError: false},
			{name: "sym of dir trailing dot dot", ctx: defaultContext, pathPairs: []SrcDest{{trailingDotDotDir, "sd9.."}}, shouldError: false},

			{name: "sym to file trailing /", ctx: defaultContext, pathPairs: []SrcDest{{symFile + "/", "sf8"}}, shouldError: false},
			{name: "sym to dir trailing /", ctx: defaultContext, pathPairs: []SrcDest{{symDir + "/", "sd10"}}, shouldError: false},
			{name: "sym to file trailing /.", ctx: defaultContext, pathPairs: []SrcDest{{symFile + "/.", "sf9"}}, shouldError: false},
			{name: "sym to dir trailing /.", ctx: defaultContext, pathPairs: []SrcDest{{symDir + "/.", "sd11"}}, shouldError: false},
			{name: "sym to file trailing /..", ctx: defaultContext, pathPairs: []SrcDest{{innerSymFile + "/..", "sf10_dir"}}, shouldError: false},
			{name: "sym to dir trailing /..", ctx: defaultContext, pathPairs: []SrcDest{{innerSymDir + "/..", "sd12"}}, shouldError: false},
			{name: "Refuse .cvmfscatalog sym file upload (error)", ctx: defaultContext, pathPairs: []SrcDest{{symFile, ".cvmfscatalog"}}, shouldError: true},
			{name: "Refuse .cvmfscatalog sym dir upload (error)", ctx: defaultContext, pathPairs: []SrcDest{{symDir, ".cvmfscatalog"}}, shouldError: true},

			{name: "sym of file to dot", ctx: defaultContext, pathPairs: []SrcDest{{symFile, "."}}, shouldError: true},
			{name: "sym of file to dot dot", ctx: defaultContext, pathPairs: []SrcDest{{symFile, ".."}}, shouldError: true},
			{name: "sym of file to path/dot", ctx: defaultContext, pathPairs: []SrcDest{{symFile, "symSetupFullDir/."}}, shouldError: true},
			{name: "sym of file to path/dot dot", ctx: defaultContext, pathPairs: []SrcDest{{symFile, "symSetupFullDir/.."}}, shouldError: true},
			{name: "sym of dir to dot", ctx: defaultContext, pathPairs: []SrcDest{{symDir, "."}}, shouldError: true},
			{name: "sym of dir to dot dot", ctx: defaultContext, pathPairs: []SrcDest{{symDir, ".."}}, shouldError: true},
			{name: "sym of dir to path/dot", ctx: defaultContext, pathPairs: []SrcDest{{symDir, "symSetupFullDir/."}}, shouldError: true},
			{name: "sym of dir to path/dot dot", ctx: defaultContext, pathPairs: []SrcDest{{symDir, "symSetupFullDir/.."}}, shouldError: true},

			{name: "sym of file over sym to file", ctx: defaultContext, pathPairs: []SrcDest{{symFile, "symSetupSymFile"}}, shouldError: false},
			{name: "sym of file over sym to dir", ctx: defaultContext, pathPairs: []SrcDest{{symFile, "symSetupSymDir"}}, shouldError: false},
			{name: "sym of file over broken sym", ctx: defaultContext, pathPairs: []SrcDest{{symFile, "symSetupSymBroken"}}, shouldError: false},
		},
		{
			{name: "sym of file over file", ctx: defaultContext, pathPairs: []SrcDest{{symFile, "sf2"}}, shouldError: false},
			{name: "sym of dir over dir", ctx: defaultContext, pathPairs: []SrcDest{{symDir, "sd1"}}, shouldError: false},
			{name: "sym of dir over full dir", ctx: defaultContext, pathPairs: []SrcDest{{symDir, "sd2"}}, shouldError: false},
		},
		{
			{name: "sym of file over empty dir", ctx: defaultContext, pathPairs: []SrcDest{{symFile, "symSetupEmptyDir"}}, shouldError: true, noErrorNoDotscheme: true},
			{name: "sym of file over full dir", ctx: defaultContext, pathPairs: []SrcDest{{symFile, "symSetupFullDir"}}, shouldError: true, noErrorNoDotscheme: true},
			{name: "sym of dir over sym to file", ctx: defaultContext, pathPairs: []SrcDest{{symDir, "symSetupSymFile2"}}, shouldError: true},
			{name: "sym of dir over sym to dir", ctx: defaultContext, pathPairs: []SrcDest{{symDir, "symSetupSymDir2"}}, shouldError: true},
			{name: "sym of dir over broken sym", ctx: defaultContext, pathPairs: []SrcDest{{symDir, "symSetupSymBroken2"}}, shouldError: true},
			{name: "sym of dir over file", ctx: defaultContext, pathPairs: []SrcDest{{symDir, "symSetupFile.txt"}}, shouldError: true},
		},
	}
	if runTest {
		reinstateExternalFuncs := setupExternalFuncs()
		addObjects(defaultContext, TestSymObjects)
		testRunner(t, tests)
		reinstateExternalFuncs()
	}
	return tests, TestSymObjects, teardownFileE2E
}

func TestE2EPermissionChecking(t *testing.T) {
	E2EPermissionCheckingHelper(t, true)
}

func E2EPermissionCheckingHelper(t *testing.T, runTest bool) ([][]Test, CreateLists, func(t *testing.T)) {
	// t.Parallel()
	// setupEnvironmentE2E()
	teardownFileDirE2E, emptyDir, differentFile, symFile, symDir := setupPermissionTestE2E(t)
	if runTest {
		defer teardownFileDirE2E(t)
	}

	tests := [][]Test{
		{
			{name: "upload to inner dir allowed", ctx: defaultContext, pathPairs: []SrcDest{{differentFile, "disallow-upload/allow-upload-inner/f1.txt"}}, shouldError: false},
			{name: "upload allowed in dir", ctx: defaultContext, pathPairs: []SrcDest{{differentFile, "allow-upload/f1.txt"}}, shouldError: false},
			{name: "upload disallowed in dir", ctx: defaultContext, pathPairs: []SrcDest{{differentFile, "disallow-upload/f1.txt"}}, shouldError: true},
			{name: "upload disallowed in inner dir", ctx: defaultContext, pathPairs: []SrcDest{{differentFile, "allow-upload/disallow-upload-inner/f1.txt"}}, shouldError: true},

			{name: "check permissions upload fail to dir", ctx: defaultContext, pathPairs: []SrcDest{{differentFile, "check-permissions/f1.txt"}}, shouldError: true},
			{name: "check permissions upload succeeds to dir", ctx: defaultContext, pathPairs: []SrcDest{{differentFile, "check-permissions2/f1.txt"}}, shouldError: false},
			{name: "check permissions upload file fails to dir", ctx: defaultContext, pathPairs: []SrcDest{{differentFile, "check-permissions2/inner-dir/f1.txt"}}, shouldError: true},
			{name: "check permissions upload dir into fails to dir", ctx: defaultContext, pathPairs: []SrcDest{{emptyDir, "check-permissions2/inner-dir/d1"}}, shouldError: true},
			{name: "check permissions upload dir fails to dir", ctx: defaultContext, pathPairs: []SrcDest{{emptyDir, "check-permissions2/inner-dir"}}, shouldError: false},
			{name: "check permissions upload sym file fails to dir", ctx: defaultContext, pathPairs: []SrcDest{{symFile, "check-permissions2/inner-dir/sf1.txt"}}, shouldError: true},
			{name: "check permissions upload sym dir fails to dir", ctx: defaultContext, pathPairs: []SrcDest{{symDir, "check-permissions2/inner-dir/sd1"}}, shouldError: true},
			{name: "check permissions upload file succeeds (can write to outer dir)", ctx: defaultContext, pathPairs: []SrcDest{{differentFile, "check-permissions2/ro-file"}}, shouldError: false},
			{name: "check permissions upload succeed file", ctx: defaultContext, pathPairs: []SrcDest{{differentFile, "check-permissions2/writeable-file"}}, shouldError: false},
			{name: "no check permissions upload succeeds file", ctx: defaultContext, pathPairs: []SrcDest{{differentFile, "no-check-permissions/ro-file"}}, shouldError: false},
			{name: "no check permissions upload succeed file", ctx: defaultContext, pathPairs: []SrcDest{{differentFile, "no-check-permissions/writeable-file"}}, shouldError: false},
			{name: "no check permissions upload file succeeds to dir", ctx: defaultContext, pathPairs: []SrcDest{{differentFile, "no-check-permissions/inner-dir/f1.txt"}}, shouldError: false},
			{name: "no check permissions upload dir succeeds to dir", ctx: defaultContext, pathPairs: []SrcDest{{emptyDir, "no-check-permissions/inner-dir"}}, shouldError: false},
			{name: "no check permissions upload dir succeeds over dir", ctx: defaultContext, pathPairs: []SrcDest{{emptyDir, "no-check-permissions/inner-dir2/d1"}}, shouldError: false},
			{name: "no check permissions upload sym file succeeds to dir", ctx: defaultContext, pathPairs: []SrcDest{{symFile, "no-check-permissions/inner-dir/sf1.txt"}}, shouldError: false},
			{name: "no check permissions upload sym dir succeeds to dir", ctx: defaultContext, pathPairs: []SrcDest{{symDir, "no-check-permissions/inner-dir/sd1"}}, shouldError: false},

			{name: "check permissions upload fail to dir acl", ctx: defaultContext, pathPairs: []SrcDest{{differentFile, "check-permissions3/f1.txt"}}, shouldError: true},
			{name: "check permissions upload file fails (can't write to outer dir) acl", ctx: defaultContext, pathPairs: []SrcDest{{differentFile, "check-permissions3/rwx-file"}}, shouldError: true},
			{name: "check permissions upload succeeds to dir acl", ctx: defaultContext, pathPairs: []SrcDest{{differentFile, "check-permissions4/f1.txt"}}, shouldError: false},
			{name: "check permissions upload file fails to dir acl", ctx: defaultContext, pathPairs: []SrcDest{{differentFile, "check-permissions4/inner-dir/f1.txt"}}, shouldError: true},
			{name: "check permissions upload dir over fails to dir acl", ctx: defaultContext, pathPairs: []SrcDest{{emptyDir, "check-permissions4/inner-dir/d1"}}, shouldError: true},
			{name: "check permissions upload dir fails to dir acl", ctx: defaultContext, pathPairs: []SrcDest{{emptyDir, "check-permissions4/inner-dir"}}, shouldError: false},
			{name: "check permissions upload sym file fails to dir acl", ctx: defaultContext, pathPairs: []SrcDest{{symFile, "check-permissions4/inner-dir/sf1.txt"}}, shouldError: true},
			{name: "check permissions upload sym dir fails to dir acl", ctx: defaultContext, pathPairs: []SrcDest{{symDir, "check-permissions4/inner-dir/sd1"}}, shouldError: true},
			{name: "check permissions upload file succeeds (can write to outer dir) acl", ctx: defaultContext, pathPairs: []SrcDest{{differentFile, "check-permissions4/ro-file"}}, shouldError: false},
			{name: "check permissions upload succeed file acl", ctx: defaultContext, pathPairs: []SrcDest{{differentFile, "check-permissions4/writeable-file"}}, shouldError: false},

			{name: "acl flag allowed", ctx: aclContext, pathPairs: []SrcDest{{emptyDir, "acl-flag/d1"}}, shouldError: false},
			{name: "acl flag not allowed", ctx: aclContext, pathPairs: []SrcDest{{emptyDir, "no-acl-flag/d1"}}, shouldError: true},
			{name: "acl flag not allowed inner", ctx: aclContext, pathPairs: []SrcDest{{emptyDir, "acl-flag/no-acl-flag/d1"}}, shouldError: true},
			{name: "acl flag not allowed, not used works", ctx: noAclContext, pathPairs: []SrcDest{{emptyDir, "no-acl-flag/d2"}}, shouldError: false},
			{name: "acl flag not allowed inner, not used works", ctx: noAclContext, pathPairs: []SrcDest{{emptyDir, "acl-flag/no-acl-flag/d2"}}, shouldError: false},

			{name: "Can still upload even though can't delete", ctx: defaultContext, pathPairs: []SrcDest{{differentFile, "disallow-delete/f1.txt"}}, shouldError: false},
		},
	}
	if runTest {
		reinstateExternalFuncs := setupExternalFuncs()
		addObjects(defaultContext, TestPermissionedObjects)
		testRunner(t, tests)
		reinstateExternalFuncs()
	}
	return tests, TestPermissionedObjects, teardownFileDirE2E
}

func getCvmfsConfInfoDefaultMock(repoName string, cvmfsConfFile *pathlib.Path) (pkg.ConfStruct, string, int, map[int]bool, error) {
	cfg, uidStr, uid, gidMap, err := pkg.GetCvmfsConfigurationInfo(repoName, pkg.GetConfigFileForRepo(repoName, OVERRIDE_CONFIG_FLAG_SET, OVERRIDE_CONFIG_PATH))
	if err != nil {
		return cfg, "", 0, nil, err
	}
	delete(cfg.Repo.GroupConfig, pkg.DefaultGroupPath)
	return cfg, uidStr, uid, gidMap, err
}

func TestE2EPermissionDefaultChecking(t *testing.T) {
	E2EPermissionDefaultCheckingHelper(t, true)
}

func E2EPermissionDefaultCheckingHelper(t *testing.T, runTest bool) ([][]Test, CreateLists, func(t *testing.T)) {
	// t.Parallel()
	// setupEnvironmentE2E()
	teardownFileDirE2E, testFile := setupPermissionDefaultTestE2E(t)
	if runTest {
		defer teardownFileDirE2E(t)
	}
	// fullDirPath := pathlib.NewPath(fullDir)
	testFile = testFile
	tests := [][]Test{
		{}, {}, {},
		{
			{name: "Can't upload", ctx: defaultContext, pathPairs: []SrcDest{{testFile, "def_f1.txt"}}, shouldError: true},
		},
	}
	if runTest {
		reinstateExternalFuncs := setupExternalFuncs()
		getCvmfsConfInfoHold := getCvmfsConfInfo
		getCvmfsConfInfo = getCvmfsConfInfoDefaultMock
		testRunner(t, tests)
		getCvmfsConfInfo = getCvmfsConfInfoHold
		reinstateExternalFuncs()
	}
	return tests, EmptyCreateLists, teardownFileDirE2E
}

func TestE2EFaclBasic(t *testing.T) {
	E2EFaclBasicHelper(t, true)
}

// ADD THE OBJECTS CREATED AS OUTPUT OF FUNCTION
func E2EFaclBasicHelper(t *testing.T, runTest bool) ([][]Test, CreateLists, func(t *testing.T)) {
	// t.Parallel()
	// setupEnvironmentE2E()
	// TODO: Can consolidate this add objects and put it at the beginning of the code. Currently that extra few seconds isn't too bad.
	t1teardownFaclE2E, aclFile1, aclFile2, aclFile3, aclFile4, aclFile5, aclFile6, aclFile7, aclFile8 := setupFaclTestsE2E(t)
	if runTest {
		defer t1teardownFaclE2E(t)
	}
	tests := [][]Test{
		{
			{name: "Mod acl (basic, same acl)", ctx: defaultContext, pathPairs: []SrcDest{{aclFile1, "acl1"}}, shouldError: false, useFacl: true},
			{name: "Mod acl, add user", ctx: defaultContext, pathPairs: []SrcDest{{aclFile2, "acl2"}}, shouldError: false, useFacl: true},
			{name: "Mod acl, add group", ctx: defaultContext, pathPairs: []SrcDest{{aclFile3, "acl3"}}, shouldError: false, useFacl: true},
			{name: "Mod acl, add group (diff gid)", ctx: defaultContext, pathPairs: []SrcDest{{aclFile4, "acl4"}}, shouldError: false, useFacl: true},
			{name: "Mod acl, add 2 groups", ctx: defaultContext, pathPairs: []SrcDest{{aclFile5, "acl5"}}, shouldError: false, useFacl: true},
			{name: "Mod acl, comment cleaned", ctx: defaultContext, pathPairs: []SrcDest{{aclFile6, "acl6"}}, shouldError: false, useFacl: true},
			{name: "Mod acl, malformed (error)", ctx: defaultContext, pathPairs: []SrcDest{{aclFile7, "acl7"}}, shouldError: true, useFacl: true},
			{name: "Mod acl, groupname", ctx: defaultContext, pathPairs: []SrcDest{{aclFile8, "acl8"}}, shouldError: false, useFacl: true},
			{name: "Mod acl, diff dirs, diff files", ctx: defaultContext, pathPairs: []SrcDest{{aclFile3, "acl9"}, {aclFile4, "acl10"}}, shouldError: false, useFacl: true},
			{name: "Mod acl, diff dirs, same file", ctx: defaultContext, pathPairs: []SrcDest{{aclFile4, "acl11"}, {aclFile4, "acl12"}}, shouldError: false, useFacl: true},
			{name: "Mod acl, Not Permissioned (error)", ctx: defaultContext, pathPairs: []SrcDest{{aclFile2, "acl14"}}, shouldError: true, useFacl: true},
			{name: "Mod acl, Not Permissioned inner (error)", ctx: defaultContext, pathPairs: []SrcDest{{aclFile2, "acl14/acl15"}}, shouldError: true, useFacl: true},
		},
	}
	if runTest {
		reinstateExternalFuncs := setupExternalFuncs()
		addObjects(defaultContext, TestFaclObjects)
		testRunner(t, tests)
		reinstateExternalFuncs()
	}
	return tests, TestFaclObjects, t1teardownFaclE2E
}

func TestMain(m *testing.M) {
	confFile := pkg.SetupConfigFile()
	OVERRIDE_CONFIG_FLAG_SET = true
	OVERRIDE_CONFIG_PATH = confFile
	defer os.Remove(confFile)
	setupContexts()
	pkg.SetupEnvironmentE2E()
	code := m.Run()
	os.Exit(code)
}
