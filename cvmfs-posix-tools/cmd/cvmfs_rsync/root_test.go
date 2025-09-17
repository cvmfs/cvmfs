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
	srcPathStrings     []string
	destPathString     string
	copyOver           bool
	shouldError        bool
	noChange           bool
	successNoDotScheme bool
	excluded           map[string]bool
	purgeList          []string
}

var (
	defaultContext,
	relativeContext,
	relativeRecursiveContext,
	recursiveContext,
	dirsContext,
	dirsDeleteContext,
	deleteContext,
	deleteRecursiveContext,
	purgeRecursiveContext,
	deletePurgeRecursiveContext,
	dirsDeletePurgeContext,
	linkDerefContext,
	linkDerefRecursiveContext,
	linkDerefRecursiveDeleteContext,
	aclRecursiveContext,
	noAclRecursiveContext Context
)

func setupContexts() {
	defaultContext = setupContext(false, false, false, false, false, false, "")
	relativeContext = setupContext(false, false, false, false, false, true, "")
	relativeRecursiveContext = setupContext(true, false, false, false, false, true, "")
	recursiveContext = setupContext(true, false, false, false, false, false, "")
	dirsContext = setupContext(false, true, false, false, false, false, "")
	dirsDeleteContext = setupContext(false, true, true, false, false, false, "")
	deleteContext = setupContext(false, false, true, false, false, false, "")
	deleteRecursiveContext = setupContext(true, false, true, false, false, false, "") // Note: delete has to be paired with recursive
	purgeRecursiveContext = func() Context { newCtx := recursiveContext; newCtx.purge = true; return newCtx }()
	deletePurgeRecursiveContext = func() Context { newCtx := deleteRecursiveContext; newCtx.purge = true; return newCtx }()
	dirsDeletePurgeContext = func() Context { newCtx := dirsDeleteContext; newCtx.purge = true; return newCtx }()
	linkDerefContext = setupContext(false, false, false, true, false, false, "")
	linkDerefRecursiveContext = setupContext(true, false, false, true, false, false, "")
	linkDerefRecursiveDeleteContext = setupContext(true, false, true, true, false, false, "")
	aclRecursiveContext = func() Context { newCtx := recursiveContext; newCtx.acls = pkg.ACLPreserveAll; return newCtx }()
	noAclRecursiveContext = func() Context { newCtx := recursiveContext; newCtx.acls = pkg.ACLNone; return newCtx }()
}

// Conveniece contexts for debugging if necessary
// var relativeDryrunContext = setupContext(false, false, false, false, true, false, true, "")
// var dryrunContext = setupContext(false, false, false, false, true, false, false, "")
// var dryrunRecursiveContext = setupContext(true, false, false, false, true, false, false, "")
// var dryrunDeleteRecursiveContext = setupContext(true, false, true, false, true, false, false, "")
// var excludeRecursiveContext = setupContext(true, false, false, false, false, false, false, "test*")

// The indexes for when tests are run are very important.
// i == 1 || 4 are for when that test would not result in a change of the file system
// i == 6 Is for testing the default permissions
func TestTurboSpeed(t *testing.T) {
	maxTestCollectionLen := 1
	testsToRun := []func(*testing.T, bool) ([][]Test, func(t *testing.T)){
		E2EFileBasicHelper,
		E2EDirBasicHelper,
		E2ESymBasicHelper,
		E2EDirRecursiveHelper,
		E2EDeleteHelper,
		E2EPurgeHelper,
		E2ESymDerefHelper,
		E2EExcludeHelper,
		E2EExcludeDeleteHelper,
		E2EExcludePurgeHelper,
		E2EPermissionCheckingHelper,
		E2EPermissionDefaultCheckingHelper,
		E2ERelativeHelper,
		E2EDirsFlagHelper,
		E2EPurgeDirsFlagHelper,
	}
	testCollections := [][][]Test{}

	for _, test := range testsToRun {
		testCollection, teardown := test(t, false)
		defer teardown(t)
		testCollections = append(testCollections, testCollection)
		maxTestCollectionLen = max(len(testCollection), maxTestCollectionLen)
	}
	// maxTestCollectionLen = 2
	reinstateExternalFuncs := setupExternalFuncs()
	for i := 0; i < maxTestCollectionLen; i++ {
		megaGraftDb, err := pkg.NewCvmfsGraftingDB()
		if err != nil {
			panic(err)
		}
		graft = pkg.Mock_mega_graft(megaGraftDb)
		var getCvmfsConfInfoHold func(repoName string, cvmfsConfFile *pathlib.Path) (pkg.ConfStruct, string, int, map[int]bool, error)
		if i == 6 { // 6 is reserved for giving no default permissions
			getCvmfsConfInfoHold = getCvmfsConfInfo
			getCvmfsConfInfo = getCvmfsConfInfoDefaultMock
		}
		if i == 1 || i == 4 { // 1 and 4 are reserved for no change
			graft = pkg.Mock_no_graft
		}
		// Run rsync for this set of tests
		for _, testCollection := range testCollections {
			if len(testCollection) > i {
				for _, tc := range testCollection[i] {
					if tc.ctx.cfg.Repo.DotScheme || tc.successNoDotScheme {
						fmt.Printf("\nRunning test %s\n", tc.name)
						purgeTeardown := setupExternalPurge(tc.purgeList)
						err := launchRsync(tc.ctx, tc.srcPathStrings, tc.destPathString)
						if tc.shouldError {
							if err == nil {
								t.Fatalf("Test should have errored.")
							}
						} else if err != nil {
							panic(err)
						}
						purgeTeardown()
					}
				}
			}
		}
		if i == 6 {
			getCvmfsConfInfo = getCvmfsConfInfoHold
		}
		if i == 1 || i == 4 {
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
		// Verify the previously ran rsync runs with the file system
		for _, testCollection := range testCollections {
			if len(testCollection) > i {
				for _, tc := range testCollection[i] {
					if !tc.noChange && !tc.shouldError && (tc.ctx.cfg.Repo.DotScheme || tc.successNoDotScheme) {
						if !verifyRsync(tc.ctx, tc.srcPathStrings, tc.destPathString, tc.copyOver, tc.excluded) {
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
			purgeTeardown := setupExternalPurge(tc.purgeList)
			if tc.noChange {
				graft = pkg.Mock_no_graft
			}
			err := launchRsync(tc.ctx, tc.srcPathStrings, tc.destPathString)
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
			purgeTeardown()
		}
		fmt.Println("Unmount Repo")
		pkg.UmountRepo()
		time.Sleep(500 * time.Millisecond)
		fmt.Println("Mount Repo")
		pkg.MountRepo()
		fmt.Println("Verifying")
		for _, tc := range tList {
			if !tc.noChange && !tc.shouldError {
				if !verifyRsync(tc.ctx, tc.srcPathStrings, tc.destPathString, tc.copyOver, tc.excluded) {
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

func E2EFileBasicHelper(t *testing.T, runTest bool) ([][]Test, func(t *testing.T)) {
	// t.Parallel()
	// setupEnvironmentE2E()
	t1teardownFileE2E, srcName, updateName, fDotName, fDotDotName, fCatalog, fInner := setupFileTestsE2E(t)
	if runTest {
		defer t1teardownFileE2E(t)
	}
	srcNamePath := pathlib.NewPath(srcName)
	fInner = fInner
	updateName = updateName
	srcNamePath = srcNamePath
	tests := [][]Test{
		{
			{name: "file to empty", ctx: defaultContext, srcPathStrings: []string{srcName}, destPathString: pkg.TestMount(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "file over empty file", ctx: defaultContext, srcPathStrings: []string{srcName}, destPathString: pkg.TestMount() + "/test_file2.txt", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "file to ghost", ctx: defaultContext, srcPathStrings: []string{srcName}, destPathString: pkg.TestMount() + "/ghost_dir/test_file.txt", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "file to ghost copies with name", ctx: defaultContext, srcPathStrings: []string{srcName}, destPathString: pkg.TestMount() + "/ghost_dir3", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "file to ghost/ copies in", ctx: defaultContext, srcPathStrings: []string{srcName}, destPathString: pkg.TestMount() + "/ghost_dir4/", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "file to ghost same files (two uploaded, error)", ctx: defaultContext, srcPathStrings: []string{srcName, srcName}, destPathString: pkg.TestMount() + "/ghost_dir2", copyOver: false, shouldError: true, noChange: false, successNoDotScheme: true},
			{name: "file to empty trailing dot", ctx: defaultContext, srcPathStrings: []string{fDotName}, destPathString: pkg.TestMount() + "/ghost_fDot", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "file to empty trailing dot dot", ctx: defaultContext, srcPathStrings: []string{fDotDotName}, destPathString: pkg.TestMount() + "/ghost_fDotDot", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "file with trailing slash non-recursive", ctx: defaultContext, srcPathStrings: []string{srcName + "/"}, destPathString: pkg.TestMount(), copyOver: false, shouldError: true, noChange: false, successNoDotScheme: true},
			{name: "file with trailing slash", ctx: recursiveContext, srcPathStrings: []string{srcName + "/"}, destPathString: pkg.TestMount(), copyOver: false, shouldError: true, noChange: false, successNoDotScheme: true},
			{name: "setting up ghost catalog file", ctx: defaultContext, srcPathStrings: []string{srcName}, destPathString: pkg.TestMount() + "/ghost_catalog_file/", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "testing with multiple paths", ctx: defaultContext, srcPathStrings: []string{srcName, updateName}, destPathString: pkg.TestMount() + "/ghost_mult_file/", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},

			{name: "file trailing /. (non-recursive error)", ctx: defaultContext, srcPathStrings: []string{srcName + "/."}, destPathString: pkg.TestMount() + "/ghost_dir_fail/", copyOver: false, shouldError: true, noChange: false, successNoDotScheme: true},
			{name: "file trailing /.. (non-recursive error)", ctx: defaultContext, srcPathStrings: []string{srcName + "/.."}, destPathString: pkg.TestMount() + "/ghost_dir_fail/", copyOver: false, shouldError: true, noChange: false, successNoDotScheme: true},
			{name: "file trailing /. (error)", ctx: recursiveContext, srcPathStrings: []string{srcName + "/."}, destPathString: pkg.TestMount() + "/ghost_dir_fail/", copyOver: false, shouldError: true, noChange: false, successNoDotScheme: true},
			// FAIL: SHOULD MAYBE NOT DO ANYTHING, PATH CLEANING SEES THIS AS THE PARENT PATH
			// {name: "file trailing /.. (error)", ctx: recursiveContext, srcPathStrings: []string{fInner + "/.."}, destPathString: pkg.TestMount()+ "/ghost_dir_fail/", copyOver: false, shouldError: true, noChange: false, successNoDotScheme: true},
			{name: "Refuse .cvmfscatalog upload (error)", ctx: defaultContext, srcPathStrings: []string{fCatalog}, destPathString: pkg.TestMount() + "/ghost_catalog_file/", copyOver: false, shouldError: true, noChange: true, successNoDotScheme: true},
		},
		{
			{name: "file over same", ctx: defaultContext, srcPathStrings: []string{srcName}, destPathString: pkg.TestMount(), copyOver: false, shouldError: false, noChange: true, successNoDotScheme: true},
		},
		{
			{name: "file to full dir", ctx: defaultContext, srcPathStrings: []string{srcName}, destPathString: pkg.TestMount() + "/ghost_dir", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "update over file", ctx: defaultContext, srcPathStrings: []string{updateName}, destPathString: pkg.TestMount() + "/" + srcNamePath.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "file delete", ctx: deleteContext, srcPathStrings: []string{srcName}, destPathString: pkg.TestMount(), copyOver: false, shouldError: true, noChange: false, successNoDotScheme: true},
		},
		{
			{name: "file recursive delete", ctx: deleteRecursiveContext, srcPathStrings: []string{srcName}, destPathString: pkg.TestMount(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
		},
	}
	if runTest {
		reinstateExternalFuncs := setupExternalFuncs()
		testRunner(t, tests)
		reinstateExternalFuncs()
	}
	return tests, t1teardownFileE2E
}

func TestE2EDirBasic(t *testing.T) {
	E2EDirBasicHelper(t, true)
}

func E2EDirBasicHelper(t *testing.T, runTest bool) ([][]Test, func(t *testing.T)) {
	// t.Parallel()
	// setupEnvironmentE2E()
	teardownFileDirE2E, fName, emptyDir, dotDir, dotDotDir, catalogDir, innerDir := setupDirTestE2E(t)
	if runTest {
		defer teardownFileDirE2E(t)
	}
	emptyDirPath := pathlib.NewPath(emptyDir)
	fNamePath := pathlib.NewPath(fName)
	hasher := pkg.NewHasher(30, pkg.IOBufferSize)
	fNameStat, err := os.Lstat(fName)
	if err != nil {
		panic(err)
	}
	hashData, err := hasher.HashFile(fNameStat, fNamePath, defaultContext.cvmfsChunkSize)
	if err != nil {
		panic(err)
	}
	tests := [][]Test{
		{
			{name: "empty dir non-recursive (error)", ctx: defaultContext, srcPathStrings: []string{emptyDir}, destPathString: pkg.TestMount(), copyOver: false, shouldError: true, noChange: false, successNoDotScheme: true},
			{name: "empty dir created", ctx: recursiveContext, srcPathStrings: []string{emptyDir}, destPathString: pkg.TestMount(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "empty dir into ghost dir", ctx: recursiveContext, srcPathStrings: []string{emptyDir}, destPathString: pkg.TestMount() + "/" + fNamePath.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "empty dir into ghost dir/", ctx: recursiveContext, srcPathStrings: []string{emptyDir}, destPathString: pkg.TestMount() + "/emptyDirGDir/", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "dir to empty trailing dot", ctx: recursiveContext, srcPathStrings: []string{dotDir}, destPathString: pkg.TestMount() + "/ghost_dotDir", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "dir to empty trailing dot dot", ctx: recursiveContext, srcPathStrings: []string{dotDotDir}, destPathString: pkg.TestMount() + "/ghost_dotDotDir", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "setting up ghost catalog dir", ctx: defaultContext, srcPathStrings: []string{fName}, destPathString: pkg.TestMount() + "/ghost_catalog_dir/", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "empty dir/.. copies empty dir", ctx: recursiveContext, srcPathStrings: []string{innerDir + "/.."}, destPathString: pkg.TestMount() + "/ghost_inner_dot_dot/", copyOver: false, shouldError: false, noChange: true, successNoDotScheme: true},
			{name: "Refuse .cvmfscatalog dir upload (error)", ctx: recursiveContext, srcPathStrings: []string{catalogDir}, destPathString: pkg.TestMount() + "/ghost_catalog_dir/", copyOver: false, shouldError: true, noChange: true, successNoDotScheme: true},
		},
		{
			{name: "empty dir over same", ctx: recursiveContext, srcPathStrings: []string{emptyDir}, destPathString: pkg.TestMount(), copyOver: false, shouldError: false, noChange: true, successNoDotScheme: true},
			{name: "empty dir/ copies nothing", ctx: recursiveContext, srcPathStrings: []string{emptyDir + "/"}, destPathString: pkg.TestMount() + "/" + emptyDirPath.Name(), copyOver: false, shouldError: false, noChange: true, successNoDotScheme: true},
			{name: "empty dir/. copies nothing", ctx: recursiveContext, srcPathStrings: []string{emptyDir + "/."}, destPathString: pkg.TestMount() + "/" + emptyDirPath.Name(), copyOver: false, shouldError: false, noChange: true, successNoDotScheme: true},
		},
		{
			{name: "empty dir copies into", ctx: recursiveContext, srcPathStrings: []string{emptyDir}, destPathString: pkg.TestMount() + "/" + emptyDirPath.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "file into empty dir", ctx: defaultContext, srcPathStrings: []string{fName}, destPathString: pkg.TestMount() + "/" + emptyDirPath.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
		},
		{
			{name: "empty dir copies over sym", ctx: recursiveContext, srcPathStrings: []string{emptyDir}, destPathString: pkg.TestMount() + "/" + emptyDirPath.Name() + "/" + fNamePath.Name(), copyOver: false, shouldError: true, noChange: false, successNoDotScheme: true}, // In non dot scheme, tests over file
			// Not a dot scheme test, should fail if no dot scheme, and above checks copy over file
			{name: "empty dir copies over file", ctx: recursiveContext, srcPathStrings: []string{emptyDir}, destPathString: pkg.TestMount() + "/" + emptyDirPath.Name() + "/" + "." + fNamePath.Name() + "." + fmt.Sprintf("%040x", hashData.Checksum), copyOver: false, shouldError: true, noChange: false, successNoDotScheme: false},
		},
	}
	if runTest {
		reinstateExternalFuncs := setupExternalFuncs()
		testRunner(t, tests)
		reinstateExternalFuncs()
	}
	return tests, teardownFileDirE2E
}

func TestE2ESymBasic(t *testing.T) {
	E2ESymBasicHelper(t, true)
}

func E2ESymBasicHelper(t *testing.T, runTest bool) ([][]Test, func(t *testing.T)) {
	// t.Parallel()
	// setupEnvironmentE2E()
	teardownFileE2E, emptyDir, fName, symDir, symFile, sameLinkDir, sameLinkDirOver, brokenSymFile, _, trailingDot, trailingDotDot, cvmfsCatalog, innerSymFile, innerSymDir := setupSymTestE2E(t)
	if runTest {
		defer teardownFileE2E(t)
	}
	symDirPath := pathlib.NewPath(symDir)
	symFilePath := pathlib.NewPath(symFile)
	brokenSymFilePath := pathlib.NewPath(brokenSymFile)
	filePath := pathlib.NewPath(fName)
	tests := [][]Test{
		{
			{name: "file and dir to empty", ctx: recursiveContext, srcPathStrings: []string{fName, emptyDir}, destPathString: pkg.TestMount(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "sym of dir to empty", ctx: defaultContext, srcPathStrings: []string{symDir}, destPathString: pkg.TestMount(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "sym of file to empty", ctx: defaultContext, srcPathStrings: []string{symFile}, destPathString: pkg.TestMount(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "sym to ghost copies with name", ctx: defaultContext, srcPathStrings: []string{symFile}, destPathString: pkg.TestMount() + "/ghost_dir5", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "sym to ghost/ copies in", ctx: defaultContext, srcPathStrings: []string{symFile}, destPathString: pkg.TestMount() + "/ghost_dir6/", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "Directory with link the same as dot link", ctx: recursiveContext, srcPathStrings: []string{sameLinkDir}, destPathString: pkg.TestMount(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "add file to get dangling dot file", ctx: recursiveContext, srcPathStrings: []string{fName}, destPathString: pkg.TestMount() + "/remove_link/name", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "sym to empty trailing dot", ctx: defaultContext, srcPathStrings: []string{trailingDot}, destPathString: pkg.TestMount() + "/ghost_dotSym", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "sym to empty trailing dot dot", ctx: defaultContext, srcPathStrings: []string{trailingDotDot}, destPathString: pkg.TestMount() + "/ghost_dotDotSym", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},

			{name: "sym to file trailing / (non-recursive error)", ctx: defaultContext, srcPathStrings: []string{symFile + "/"}, destPathString: pkg.TestMount() + "/ghost_dir_fail/", copyOver: false, shouldError: true, noChange: false, successNoDotScheme: true},
			{name: "sym to file trailing /. (non-recursive error)", ctx: defaultContext, srcPathStrings: []string{symFile + "/."}, destPathString: pkg.TestMount() + "/ghost_dir_fail/", copyOver: false, shouldError: true, noChange: false, successNoDotScheme: true},
			{name: "sym to file trailing /.. (non-recursive error)", ctx: defaultContext, srcPathStrings: []string{innerSymFile + "/.."}, destPathString: pkg.TestMount() + "/ghost_dir_fail/", copyOver: false, shouldError: true, noChange: false, successNoDotScheme: true},
			{name: "sym to file trailing / (error)", ctx: recursiveContext, srcPathStrings: []string{symFile + "/"}, destPathString: pkg.TestMount() + "/ghost_dir_fail/", copyOver: false, shouldError: true, noChange: false, successNoDotScheme: true},
			{name: "sym to file trailing /. (error)", ctx: recursiveContext, srcPathStrings: []string{symFile + "/."}, destPathString: pkg.TestMount() + "/ghost_dir_fail/", copyOver: false, shouldError: true, noChange: false, successNoDotScheme: true},
			// FAIL: PLEASE FIX
			// {name: "sym to file trailing /.. (error)", ctx: recursiveContext, srcPathStrings: []string{innerSymFile + "/.."}, destPathString: pkg.TestMount()+ "/ghost_dir_fail/", copyOver: false, shouldError: true, noChange: false, successNoDotScheme: true},
			{name: "sym to dir trailing / (non-recursive error)", ctx: defaultContext, srcPathStrings: []string{symDir + "/"}, destPathString: pkg.TestMount() + "/ghost_dir_fail/", copyOver: false, shouldError: true, noChange: false, successNoDotScheme: true},
			{name: "sym to dir trailing /. (non-recursive error)", ctx: defaultContext, srcPathStrings: []string{symDir + "/."}, destPathString: pkg.TestMount() + "/ghost_dir_fail/", copyOver: false, shouldError: true, noChange: false, successNoDotScheme: true},
			{name: "sym to dir trailing /.. (non-recursive error)", ctx: defaultContext, srcPathStrings: []string{innerSymDir + "/.."}, destPathString: pkg.TestMount() + "/ghost_dir_fail/", copyOver: false, shouldError: true, noChange: false, successNoDotScheme: true},
			{name: "sym to dir trailing /", ctx: recursiveContext, srcPathStrings: []string{symDir + "/"}, destPathString: pkg.TestMount() + "/ghost_trailing_sym_dir/", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "sym to dir trailing /.", ctx: recursiveContext, srcPathStrings: []string{symDir + "/."}, destPathString: pkg.TestMount() + "/ghost_dot_sym_dir/", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "sym to dir trailing /..", ctx: recursiveContext, srcPathStrings: []string{innerSymDir + "/.."}, destPathString: pkg.TestMount() + "/ghost_dot_dot_sym_dir/", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},

			{name: "setting up ghost catalog dir", ctx: defaultContext, srcPathStrings: []string{fName}, destPathString: pkg.TestMount() + "/ghost_catalog_sym/", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "Refuse .cvmfscatalog sym upload (error)", ctx: defaultContext, srcPathStrings: []string{cvmfsCatalog}, destPathString: pkg.TestMount() + "/ghost_catalog_sym/", copyOver: false, shouldError: true, noChange: true, successNoDotScheme: true},
		},
		{
			{name: "sym of dir to empty over same", ctx: defaultContext, srcPathStrings: []string{symDir}, destPathString: pkg.TestMount(), copyOver: false, shouldError: false, noChange: true, successNoDotScheme: true},
			{name: "sym of file to empty over same", ctx: defaultContext, srcPathStrings: []string{symFile}, destPathString: pkg.TestMount(), copyOver: false, shouldError: false, noChange: true, successNoDotScheme: true},
			{name: "Same name over same", ctx: recursiveContext, srcPathStrings: []string{sameLinkDir}, destPathString: pkg.TestMount(), copyOver: false, shouldError: false, noChange: true, successNoDotScheme: true},
		},
		{
			{name: "broken sym, sym of file and file to ghost dir", ctx: defaultContext, srcPathStrings: []string{fName, symFile, brokenSymFile}, destPathString: pkg.TestMount() + "/ghost_dir_sym_test", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "sym of file into dir", ctx: defaultContext, srcPathStrings: []string{symFile}, destPathString: pkg.TestMount() + "/" + symDirPath.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "sym of dir over file sym", ctx: defaultContext, srcPathStrings: []string{symDir}, destPathString: pkg.TestMount() + "/" + symFilePath.Name(), copyOver: true, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "sym delete prep", ctx: deleteContext, srcPathStrings: []string{symFile}, destPathString: pkg.TestMount() + "/ghost_dir6", copyOver: false, shouldError: true, noChange: false, successNoDotScheme: true},
			// These 2 tests depend on each other and MUST be verified at the same time
			{name: "Same name over same", ctx: recursiveContext, srcPathStrings: []string{sameLinkDir}, destPathString: pkg.TestMount(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: false},
			{name: "Directory with link the same as dot link, file change", ctx: recursiveContext, srcPathStrings: []string{sameLinkDirOver + "/"}, destPathString: pkg.TestMount() + "/SameLinkName", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "Get dangling dot file", ctx: recursiveContext, srcPathStrings: []string{symDir}, destPathString: pkg.TestMount() + "/remove_link/name", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
		},
		{
			{name: "sym of dir over file", ctx: defaultContext, srcPathStrings: []string{symDir}, destPathString: pkg.TestMount() + "/" + filePath.Name(), copyOver: true, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "file copied into sym dir", ctx: defaultContext, srcPathStrings: []string{fName}, destPathString: pkg.TestMount() + "/" + symFilePath.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "file copies over sym file", ctx: defaultContext, srcPathStrings: []string{fName}, destPathString: pkg.TestMount() + "/ghost_dir_sym_test/" + symFilePath.Name(), copyOver: true, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "file copies over broken sym file", ctx: defaultContext, srcPathStrings: []string{fName}, destPathString: pkg.TestMount() + "/ghost_dir_sym_test/" + brokenSymFilePath.Name(), copyOver: true, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "sym delete", ctx: deleteContext, srcPathStrings: []string{symFile}, destPathString: pkg.TestMount(), copyOver: false, shouldError: true, noChange: false, successNoDotScheme: true},
			{name: "remove dangling dot file and underlying file", ctx: deleteRecursiveContext, srcPathStrings: []string{emptyDir + "/"}, destPathString: pkg.TestMount() + "/remove_link", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
		},
		{
			{name: "sym recursive delete", ctx: deleteRecursiveContext, srcPathStrings: []string{symFile}, destPathString: pkg.TestMount() + "/ghost_dir6", copyOver: false, shouldError: false, noChange: true, successNoDotScheme: true},
		},
	}
	if runTest {
		reinstateExternalFuncs := setupExternalFuncs()
		testRunner(t, tests)
		reinstateExternalFuncs()
	}
	return tests, teardownFileE2E
}

func TestE2EDirRecursive(t *testing.T) {
	E2EDirRecursiveHelper(t, true)
}

func E2EDirRecursiveHelper(t *testing.T, runTest bool) ([][]Test, func(t *testing.T)) {
	// t.Parallel()
	// setupEnvironmentE2E()
	teardownFileDirE2E, fullDir, _, _, _, _, _, fullDirDotStuff, _, _, _ := setupDirRecursiveTestE2E(t)
	if runTest {
		defer teardownFileDirE2E(t)
	}
	fullDirPath := pathlib.NewPath(fullDir)
	//fullDirDotStuffPath := pathlib.NewPath(fullDirDotStuff)
	tests := [][]Test{
		{
			{name: "full dir non-recursive (error)", ctx: defaultContext, srcPathStrings: []string{fullDir}, destPathString: pkg.TestMount(), copyOver: false, shouldError: true, noChange: false, successNoDotScheme: true},
			{name: "full dir recursive", ctx: recursiveContext, srcPathStrings: []string{fullDir}, destPathString: pkg.TestMount(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "full dir to ghost copies in", ctx: recursiveContext, srcPathStrings: []string{fullDir}, destPathString: pkg.TestMount() + "/ghost_dir7", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "full dir to ghost/ copies in", ctx: recursiveContext, srcPathStrings: []string{fullDir}, destPathString: pkg.TestMount() + "/ghost_dir8/", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "copy properly with .", ctx: recursiveContext, srcPathStrings: []string{fullDir + "/."}, destPathString: pkg.TestMount() + "/ghost_dir9" + fullDirPath.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "copy properly with ..", ctx: recursiveContext, srcPathStrings: []string{fullDir + "/inner_dir/.."}, destPathString: pkg.TestMount() + "/ghost_dir10" + fullDirPath.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "copy properly with weird path names", ctx: recursiveContext, srcPathStrings: []string{fullDirDotStuff}, destPathString: pkg.TestMount() + "/ghost_dir_dot_stuff" + fullDirPath.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
/*
			{name: "copy properly with trailing spaces", ctx: recursiveContext, srcPathStrings: []string{fullDirTrailingSpace}, destPathString: pkg.TestMount() + "/ghost_dir_trailing_spaces" + fullDirPath.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "names clash post hash, should error", ctx: recursiveContext, srcPathStrings: []string{fullDirNameClash}, destPathString: pkg.TestMount() + "/ghost_dir9/", copyOver: false, shouldError: true, noChange: true, successNoDotScheme: false},
*/
		},
/*
		{
			{name: "full dir recursive over same", ctx: recursiveContext, srcPathStrings: []string{fullDir}, destPathString: pkg.TestMount(), copyOver: false, shouldError: false, noChange: true, successNoDotScheme: true},
			{name: "skip copying in catalogs", ctx: recursiveContext, srcPathStrings: []string{fullDirCatalogStuff + "/"}, destPathString: pkg.TestMount() + "/ghost_dir_dot_stuff" + fullDirPath.Name() + "/" + fullDirDotStuffPath.Name() + "/", copyOver: false, shouldError: false, noChange: true, successNoDotScheme: true},
		},
		{
			{name: "full dir recursive update", ctx: recursiveContext, srcPathStrings: []string{fullDirUpdate + "/"}, destPathString: pkg.TestMount() + "/" + fullDirPath.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
		},
		{
			{name: "full dir recursive replace", ctx: recursiveContext, srcPathStrings: []string{fullDirReplace + "/"}, destPathString: pkg.TestMount() + "/" + fullDirPath.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
		},
		{
			{name: "full dir recursive error replace", ctx: recursiveContext, srcPathStrings: []string{fullDirErrReplace + "/"}, destPathString: pkg.TestMount() + "/" + fullDirPath.Name(), copyOver: false, shouldError: true, noChange: false, successNoDotScheme: true},
			{name: "full dir recursive error sym replace (copy over full dir)", ctx: recursiveContext, srcPathStrings: []string{fullDirErrSymReplace + "/"}, destPathString: pkg.TestMount() + "/" + fullDirPath.Name(), copyOver: false, shouldError: true, noChange: false, successNoDotScheme: true},
		},
		{
			{name: "full dir recursive sym replace (copy over everything else)", ctx: recursiveContext, srcPathStrings: []string{fullDirSymReplace + "/"}, destPathString: pkg.TestMount() + "/" + fullDirPath.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
		},
*/
	}
	if runTest {
		reinstateExternalFuncs := setupExternalFuncs()
		testRunner(t, tests)
		reinstateExternalFuncs()
	}
	return tests, teardownFileDirE2E
}

func TestE2EDelete(t *testing.T) {
	E2EDeleteHelper(t, true)
}

func E2EDeleteHelper(t *testing.T, runTest bool) ([][]Test, func(t *testing.T)) {
	// t.Parallel()
	// setupEnvironmentE2E()
	teardownFileDirE2E, fullDir, fullDirDelete, fullDirFiles, fullDirEmptyDirs, fullDirFullDirs, fullDirSyms, fullDirDotCheck, fullDirDotUpdate, fullDirDotFinal, fullDirOneInner := setupDeleteTestE2E(t)
	if runTest {
		defer teardownFileDirE2E(t)
	}
	fullDirPath := pathlib.NewPath(fullDir)
	// Can maybe test running delete on a file and a sym too.
	tests := [][]Test{
		{
			{name: "delete non-recursive", ctx: deleteContext, srcPathStrings: []string{fullDir}, destPathString: pkg.TestMount(), copyOver: false, shouldError: true, noChange: false, successNoDotScheme: true},
			{name: "copy in main dir", ctx: deleteRecursiveContext, srcPathStrings: []string{fullDir}, destPathString: pkg.TestMount(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "copy in filesOver", ctx: recursiveContext, srcPathStrings: []string{fullDir + "/"}, destPathString: pkg.TestMount() + "/filesOver" + fullDirPath.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "copy in emptyDirOver", ctx: recursiveContext, srcPathStrings: []string{fullDir + "/"}, destPathString: pkg.TestMount() + "/emptyDirsOver" + fullDirPath.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "copy in fullDirOver", ctx: recursiveContext, srcPathStrings: []string{fullDir + "/"}, destPathString: pkg.TestMount() + "/fullDirsOver" + fullDirPath.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "copy in symOver", ctx: recursiveContext, srcPathStrings: []string{fullDir + "/"}, destPathString: pkg.TestMount() + "/symsOver" + fullDirPath.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "dot file delete setup", ctx: recursiveContext, srcPathStrings: []string{fullDirDotCheck + "/"}, destPathString: pkg.TestMount() + "/fullDirDotCheck" + fullDirPath.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "copy in dotDel", ctx: recursiveContext, srcPathStrings: []string{fullDir + "/"}, destPathString: pkg.TestMount() + "/dotDel" + fullDirPath.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "copy in dotDotDel", ctx: recursiveContext, srcPathStrings: []string{fullDir + "/"}, destPathString: pkg.TestMount() + "/dotDotDel" + fullDirPath.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
		},
		{},
		{
			{name: "full dir recursive delete", ctx: deleteRecursiveContext, srcPathStrings: []string{fullDirDelete + "/"}, destPathString: pkg.TestMount() + "/" + fullDirPath.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "full dir file replace", ctx: deleteRecursiveContext, srcPathStrings: []string{fullDirFiles + "/"}, destPathString: pkg.TestMount() + "/filesOver" + fullDirPath.Name(), copyOver: true, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "full dir empty dir replace", ctx: deleteRecursiveContext, srcPathStrings: []string{fullDirEmptyDirs + "/"}, destPathString: pkg.TestMount() + "/emptyDirsOver" + fullDirPath.Name(), copyOver: true, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "full dir full dir replace", ctx: deleteRecursiveContext, srcPathStrings: []string{fullDirFullDirs + "/"}, destPathString: pkg.TestMount() + "/fullDirsOver" + fullDirPath.Name(), copyOver: true, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "full dir sym replace", ctx: deleteRecursiveContext, srcPathStrings: []string{fullDirSyms + "/"}, destPathString: pkg.TestMount() + "/symsOver" + fullDirPath.Name(), copyOver: true, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "dot file delete setup 2", ctx: recursiveContext, srcPathStrings: []string{fullDirDotUpdate + "/"}, destPathString: pkg.TestMount() + "/fullDirDotCheck" + fullDirPath.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "delete properly with .", ctx: deleteRecursiveContext, srcPathStrings: []string{fullDirOneInner + "/."}, destPathString: pkg.TestMount() + "/dotDel" + fullDirPath.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "delete properly with ..", ctx: deleteRecursiveContext, srcPathStrings: []string{fullDirOneInner + "/inner_dir/.."}, destPathString: pkg.TestMount() + "/dotDotDel" + fullDirPath.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
		},
		{
			{name: "dot file delete check", ctx: deleteRecursiveContext, srcPathStrings: []string{fullDirDotFinal + "/"}, destPathString: pkg.TestMount() + "/fullDirDotCheck" + fullDirPath.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
		},
		{
			{name: "full dir recursive delete over same", ctx: deleteRecursiveContext, srcPathStrings: []string{fullDirDelete + "/"}, destPathString: pkg.TestMount() + "/" + fullDirPath.Name(), copyOver: false, shouldError: false, noChange: true, successNoDotScheme: true},
		},
	}
	if runTest {
		reinstateExternalFuncs := setupExternalFuncs()
		testRunner(t, tests)
		reinstateExternalFuncs()
	}
	return tests, teardownFileDirE2E
}

func TestE2EPurge(t *testing.T) {
	E2EPurgeHelper(t, true)
}

func E2EPurgeHelper(t *testing.T, runTest bool) ([][]Test, func(t *testing.T)) {
	// t.Parallel()
	// setupEnvironmentE2E()
	teardownFileDirE2E, fullDir, fullDirDelete, fullDirFiles, fullDirEmptyDirs, fullDirFullDirs, fullDirSyms, fullDirDotCheck, fullDirDotUpdate, fullDirDotFinal, fullDirOneInner := setupDeleteTestE2E(t)
	if runTest {
		defer teardownFileDirE2E(t)
	}
	fullDirPath := pathlib.NewPath(fullDir)
	stdPurgeList := func(prefix string) []string {
		if recursiveContext.cfg.Repo.DotScheme {
			return []string{prefix + fullDirPath.Name() + "/.test_file.txt.f24d7b797432a7aaf05c29e032faa297277a14f8", prefix + fullDirPath.Name() + "/inner_dir/.inner_test_file.txt.b07639191cc12b8682acadeb5e6d4a67e567f068"}
		} else {
			return []string{prefix + fullDirPath.Name() + "/test_file.txt", prefix + fullDirPath.Name() + "/inner_dir/inner_test_file.txt"}
		}
	}
	filesOverPurgeList := stdPurgeList("filesOver")
	if !recursiveContext.cfg.Repo.DotScheme {
		filesOverPurgeList = filesOverPurgeList[1:]
	}
	// Can maybe test running delete on a file and a sym too.
	tests := [][]Test{
		{
			{name: "purge non-delete", ctx: purgeRecursiveContext, srcPathStrings: []string{fullDir}, destPathString: pkg.TestMount(), copyOver: false, shouldError: true, noChange: false, successNoDotScheme: true},
			{name: "copy in main dir purge", ctx: deleteRecursiveContext, srcPathStrings: []string{fullDir}, destPathString: pkg.TestMount(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "copy in filesOver purge", ctx: recursiveContext, srcPathStrings: []string{fullDir + "/"}, destPathString: pkg.TestMount() + "/filesOver" + fullDirPath.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "copy in emptyDirOver purge", ctx: recursiveContext, srcPathStrings: []string{fullDir + "/"}, destPathString: pkg.TestMount() + "/emptyDirsOver" + fullDirPath.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "copy in fullDirOver purge", ctx: recursiveContext, srcPathStrings: []string{fullDir + "/"}, destPathString: pkg.TestMount() + "/fullDirsOver" + fullDirPath.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "copy in symOver purge", ctx: recursiveContext, srcPathStrings: []string{fullDir + "/"}, destPathString: pkg.TestMount() + "/symsOver" + fullDirPath.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "dot file delete setup purge", ctx: recursiveContext, srcPathStrings: []string{fullDirDotCheck + "/"}, destPathString: pkg.TestMount() + "/fullDirDotCheck" + fullDirPath.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "copy in dotDel", ctx: recursiveContext, srcPathStrings: []string{fullDir + "/"}, destPathString: pkg.TestMount() + "/dotDel" + fullDirPath.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "copy in dotDotDel", ctx: recursiveContext, srcPathStrings: []string{fullDir + "/"}, destPathString: pkg.TestMount() + "/dotDotDel" + fullDirPath.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
		},
		{},
		{
			{name: "full dir recursive purge", ctx: deletePurgeRecursiveContext, srcPathStrings: []string{fullDirDelete + "/"}, destPathString: pkg.TestMount() + "/" + fullDirPath.Name(), purgeList: stdPurgeList(""), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: false},
			{name: "full dir file replace purge", ctx: deletePurgeRecursiveContext, srcPathStrings: []string{fullDirFiles + "/"}, destPathString: pkg.TestMount() + "/filesOver" + fullDirPath.Name(), purgeList: filesOverPurgeList, copyOver: true, shouldError: false, noChange: false, successNoDotScheme: false},
			{name: "full dir empty dir replace purge", ctx: deletePurgeRecursiveContext, srcPathStrings: []string{fullDirEmptyDirs + "/"}, destPathString: pkg.TestMount() + "/emptyDirsOver" + fullDirPath.Name(), purgeList: stdPurgeList("emptyDirsOver"), copyOver: true, shouldError: false, noChange: false, successNoDotScheme: false},
			{name: "full dir full dir replace purge", ctx: deletePurgeRecursiveContext, srcPathStrings: []string{fullDirFullDirs + "/"}, destPathString: pkg.TestMount() + "/fullDirsOver" + fullDirPath.Name(), purgeList: stdPurgeList("fullDirsOver"), copyOver: true, shouldError: false, noChange: false, successNoDotScheme: false},
			{name: "full dir sym replace purge", ctx: deletePurgeRecursiveContext, srcPathStrings: []string{fullDirSyms + "/"}, destPathString: pkg.TestMount() + "/symsOver" + fullDirPath.Name(), purgeList: stdPurgeList("symsOver"), copyOver: true, shouldError: false, noChange: false, successNoDotScheme: false},
			{name: "dot file delete setup 2", ctx: recursiveContext, srcPathStrings: []string{fullDirDotUpdate + "/"}, destPathString: pkg.TestMount() + "/fullDirDotCheck" + fullDirPath.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: false},
			{name: "purge properly with .", ctx: deleteRecursiveContext, srcPathStrings: []string{fullDirOneInner + "/."}, destPathString: pkg.TestMount() + "/dotDel" + fullDirPath.Name(), purgeList: stdPurgeList("dotDel"), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: false},
			{name: "purge properly with ..", ctx: deleteRecursiveContext, srcPathStrings: []string{fullDirOneInner + "/inner_dir/.."}, destPathString: pkg.TestMount() + "/dotDotDel" + fullDirPath.Name(), purgeList: stdPurgeList("dotDotDel"), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: false},
		},
		{
			{name: "dot file delete check purge", ctx: deletePurgeRecursiveContext, srcPathStrings: []string{fullDirDotFinal + "/"}, destPathString: pkg.TestMount() + "/fullDirDotCheck" + fullDirPath.Name(), purgeList: []string{"fullDirDotCheck" + fullDirPath.Name() + "/.test_file1.txt.8565823b44213e811cea44c06e0e45f0ca59d5e2", "fullDirDotCheck" + fullDirPath.Name() + "/.test_file1.txt.8d4fd5b53e0c00a1b3eeb1e61e7aacac46f43f90", "fullDirDotCheck" + fullDirPath.Name() + "/.test_file2.txt.8565823b44213e811cea44c06e0e45f0ca59d5e2"}, copyOver: false, shouldError: false, noChange: false, successNoDotScheme: false},
		},
		{
			{name: "full dir recursive delete over same", ctx: deletePurgeRecursiveContext, srcPathStrings: []string{fullDirDelete + "/"}, destPathString: pkg.TestMount() + "/" + fullDirPath.Name(), copyOver: false, shouldError: false, noChange: true, successNoDotScheme: false},
		},
	}
	if runTest {
		reinstateExternalFuncs := setupExternalFuncs()
		testRunner(t, tests)
		reinstateExternalFuncs()
	}
	return tests, teardownFileDirE2E
}

func TestE2ESymDeref(t *testing.T) {
	E2ESymDerefHelper(t, true)
}

func E2ESymDerefHelper(t *testing.T, runTest bool) ([][]Test, func(t *testing.T)) {
	// t.Parallel()
	// setupEnvironmentE2E()
	teardownFileE2E, emptyDir, emptyDir2, emptyDir3, fullDir, fullDir2, fullDir3, fullDir4, symFile, symEmptyDir, symFullDir, fullDirSymFile, fullDirSymFileDelete, fullDirSymEmptyDir, fullDirSymFullDir := setupSymDerefTestE2E(t)
	if runTest {
		defer teardownFileE2E(t)
	}
	emptyDirPath := pathlib.NewPath(emptyDir)
	emptyDirPath2 := pathlib.NewPath(emptyDir2)
	emptyDirPath3 := pathlib.NewPath(emptyDir3)
	fullDirPath2 := pathlib.NewPath(fullDir2)
	fullDirPath3 := pathlib.NewPath(fullDir3)
	fullDirPath4 := pathlib.NewPath(fullDir4)
	tests := [][]Test{
		{
			{name: "deref symfile to empty", ctx: linkDerefContext, srcPathStrings: []string{symFile}, destPathString: pkg.TestMount(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "deref symEmptyDir to empty (non-recursive, error)", ctx: linkDerefContext, srcPathStrings: []string{symEmptyDir}, destPathString: pkg.TestMount(), copyOver: false, shouldError: true, noChange: false, successNoDotScheme: true},
			{name: "deref symEmptyDir to empty", ctx: linkDerefRecursiveContext, srcPathStrings: []string{symEmptyDir}, destPathString: pkg.TestMount(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "deref symFullDir to empty", ctx: linkDerefRecursiveContext, srcPathStrings: []string{symFullDir}, destPathString: pkg.TestMount(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},

			{name: "copy setup dirs in", ctx: recursiveContext, srcPathStrings: []string{emptyDir, emptyDir2, emptyDir3, fullDir2, fullDir3, fullDir4}, destPathString: pkg.TestMount(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},

			// Test without recursion trying to copy over these^

			{name: "Setup symFilesOver directory", ctx: recursiveContext, srcPathStrings: []string{fullDir + "/"}, destPathString: pkg.TestMount() + "/symFilesOver", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},             // Will have to test separate? Will error on copy over full
			{name: "Setup symFilesOverDelete directory", ctx: recursiveContext, srcPathStrings: []string{fullDir + "/"}, destPathString: pkg.TestMount() + "/symFilesOverDelete", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true}, // Will have to test separate? Will error on copy over full
			{name: "Setup symEmptyDirsOver directory", ctx: recursiveContext, srcPathStrings: []string{fullDir + "/"}, destPathString: pkg.TestMount() + "/symEmptyDirsOver", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "Setup symFullDirsOver directory", ctx: recursiveContext, srcPathStrings: []string{fullDir + "/"}, destPathString: pkg.TestMount() + "/symFullDirsOver", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
		},
		{
			{name: "deref symfile over same", ctx: linkDerefContext, srcPathStrings: []string{symFile}, destPathString: pkg.TestMount(), copyOver: false, shouldError: false, noChange: true, successNoDotScheme: true},
			{name: "deref symEmptyDir over same", ctx: linkDerefRecursiveContext, srcPathStrings: []string{symEmptyDir}, destPathString: pkg.TestMount(), copyOver: false, shouldError: false, noChange: true, successNoDotScheme: true},
			{name: "deref symFullDir over same", ctx: linkDerefRecursiveContext, srcPathStrings: []string{symFullDir}, destPathString: pkg.TestMount(), copyOver: false, shouldError: false, noChange: true, successNoDotScheme: true},
		},
		{
			{name: "deref symfile to empty dir", ctx: linkDerefContext, srcPathStrings: []string{symFile}, destPathString: pkg.TestMount() + "/" + emptyDirPath.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "deref symEmptyDir to empty dir", ctx: linkDerefRecursiveContext, srcPathStrings: []string{symEmptyDir}, destPathString: pkg.TestMount() + "/" + emptyDirPath2.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "deref symFullDir to empty dir", ctx: linkDerefRecursiveContext, srcPathStrings: []string{symFullDir}, destPathString: pkg.TestMount() + "/" + emptyDirPath3.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},

			{name: "deref symfile to full dir", ctx: linkDerefContext, srcPathStrings: []string{symFile}, destPathString: pkg.TestMount() + "/" + fullDirPath2.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "deref symEmptyDir to full dir", ctx: linkDerefRecursiveContext, srcPathStrings: []string{symEmptyDir}, destPathString: pkg.TestMount() + "/" + fullDirPath3.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "deref symFullDir to full dir", ctx: linkDerefRecursiveContext, srcPathStrings: []string{symFullDir}, destPathString: pkg.TestMount() + "/" + fullDirPath4.Name(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},

			// The files over are sym -> sym -> file
			{name: "Copy over symFilesOver dir", ctx: linkDerefRecursiveContext, srcPathStrings: []string{fullDirSymFile + "/"}, destPathString: pkg.TestMount() + "/symFilesOver", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},                                                                  // Will have to test separate? Will error on copy over full
			{name: "Copy over symFilesOverDelete with delete (checks recursive over full dir)", ctx: linkDerefRecursiveDeleteContext, srcPathStrings: []string{fullDirSymFileDelete + "/"}, destPathString: pkg.TestMount() + "/symFilesOverDelete", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true}, // Will have to test separate? Will error on copy over full
			{name: "Copy over symEmptyDirsOver dir", ctx: linkDerefRecursiveContext, srcPathStrings: []string{fullDirSymEmptyDir + "/"}, destPathString: pkg.TestMount() + "/symEmptyDirsOver", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "Copy over symFullDirsOver dir", ctx: linkDerefRecursiveContext, srcPathStrings: []string{fullDirSymFullDir + "/"}, destPathString: pkg.TestMount() + "/symFullDirsOver", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
		},
	}
	if runTest {
		reinstateExternalFuncs := setupExternalFuncs()
		testRunner(t, tests)
		reinstateExternalFuncs()
	}
	return tests, teardownFileE2E
}

func TestE2EExclude(t *testing.T) {
	E2EExcludeHelper(t, true)
}

func E2EExcludeHelper(t *testing.T, runTest bool) ([][]Test, func(t *testing.T)) {
	// t.Parallel()
	// setupEnvironmentE2E()
	teardownFileDirE2E, fullDir, fullDirMulti := setupExcludeTestE2E(t)
	if runTest {
		defer teardownFileDirE2E(t)
	}
	fullDirPath := pathlib.NewPath(fullDir)
	tests := [][]Test{
		{
			{name: "exclude file sym dir", ctx: setupContext(true, false, false, false, false, false, "exc_test*"), srcPathStrings: []string{fullDir + "/"}, destPathString: pkg.TestMount() + "/fullDirExclude0", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true,
				excluded: map[string]bool{pkg.TestMount() + "/fullDirExclude0/exc_test_file.txt": true, pkg.TestMount() + "/fullDirExclude0/exc_test_dir": true, pkg.TestMount() + "/fullDirExclude0/exc_test_file_sym.txt": true}},
			{name: "exclude matches nothing", ctx: setupContext(true, false, false, false, false, false, "foobar"), srcPathStrings: []string{fullDir + "/"}, destPathString: pkg.TestMount() + "/fullDirExclude.5", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "exclude file sym", ctx: setupContext(true, false, false, false, false, false, "exc_test_file*"), srcPathStrings: []string{fullDir + "/"}, destPathString: pkg.TestMount() + "/fullDirExclude1", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true,
				excluded: map[string]bool{pkg.TestMount() + "/fullDirExclude1/exc_test_file.txt": true, pkg.TestMount() + "/fullDirExclude1/exc_test_file_sym.txt": true}},
			{name: "exclude dir", ctx: setupContext(true, false, false, false, false, false, "exc_test_di?"), srcPathStrings: []string{fullDir + "/"}, destPathString: pkg.TestMount() + "/fullDirExclude2", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true,
				excluded: map[string]bool{pkg.TestMount() + "/fullDirExclude2/exc_test_dir": true}},
			{name: "exclude base dir", ctx: setupContext(true, false, false, false, false, false, fullDirPath.Name()), srcPathStrings: []string{fullDir}, destPathString: pkg.TestMount(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true,
				excluded: map[string]bool{pkg.TestMount() + "/" + fullDirPath.Name(): true}},
			{name: "exclude base file", ctx: setupContext(true, false, false, false, false, false, "exc_test_file.txt"), srcPathStrings: []string{fullDir + "/exc_test_file.txt"}, destPathString: pkg.TestMount(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true,
				excluded: map[string]bool{pkg.TestMount() + "/exc_test_file.txt": true}},
			{name: "exclude base sym", ctx: setupContext(true, false, false, false, false, false, "exc_test_file_sym.txt"), srcPathStrings: []string{fullDir + "/exc_test_file_sym.txt"}, destPathString: pkg.TestMount(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true,
				excluded: map[string]bool{pkg.TestMount() + "/exc_test_file_sym.txt": true}},
			{name: "exclude path multi-level", ctx: setupContext(true, false, false, false, false, false, "exc_test_file.txt"), srcPathStrings: []string{fullDirMulti + "/"}, destPathString: pkg.TestMount() + "/fullDirExclude3", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true,
				excluded: map[string]bool{pkg.TestMount() + "/fullDirExclude3/exc_test_file.txt": true, pkg.TestMount() + "/fullDirExclude3/exc_test_dir/exc_test_file.txt": true}},
			{name: "exclude 2 path multi-level", ctx: setupContext(true, false, false, false, false, false, "exc_test_dir/exc_test_fil?.txt"), srcPathStrings: []string{fullDirMulti + "/"}, destPathString: pkg.TestMount() + "/fullDirExclude4", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true,
				excluded: map[string]bool{pkg.TestMount() + "/fullDirExclude4/exc_test_dir/exc_test_dir/exc_test_filt.txt": true, pkg.TestMount() + "/fullDirExclude4/exc_test_dir/exc_test_file.txt": true}},
			{name: "exclude 2 path match multi-level", ctx: setupContext(true, false, false, false, false, false, "exc_test_*/exc_test_fil?.txt"), srcPathStrings: []string{fullDirMulti + "/"}, destPathString: pkg.TestMount() + "/fullDirExclude5", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true,
				excluded: map[string]bool{pkg.TestMount() + "/fullDirExclude5/exc_test_dir/exc_test_dir/exc_test_filt.txt": true, pkg.TestMount() + "/fullDirExclude5/exc_test_dir/exc_test_file.txt": true}},
			{name: "exclude 3 path match", ctx: setupContext(true, false, false, false, false, false, "exc_test_dir/exc_test_*/exc_test_fil?.txt"), srcPathStrings: []string{fullDirMulti + "/"}, destPathString: pkg.TestMount() + "/fullDirExclude6", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true,
				excluded: map[string]bool{pkg.TestMount() + "/fullDirExclude6/exc_test_dir/exc_test_dir/exc_test_filt.txt": true}},
		},
	}
	if runTest {
		reinstateExternalFuncs := setupExternalFuncs()
		testRunner(t, tests)
		reinstateExternalFuncs()
	}
	return tests, teardownFileDirE2E
}

func TestE2EExcludeDelete(t *testing.T) {
	E2EExcludeDeleteHelper(t, true)
}

func E2EExcludeDeleteHelper(t *testing.T, runTest bool) ([][]Test, func(t *testing.T)) {
	// t.Parallel()
	// setupEnvironmentE2E()
	teardownFileDirE2E, fullDir, fullDirMulti, fullDirMultiDel := setupExcludeDeleteTestE2E(t)
	if runTest {
		defer teardownFileDirE2E(t)
	}
	// fullDirPath := pathlib.NewPath(fullDir)
	tests := [][]Test{
		{
			{name: "setup dir for exclude dir delete", ctx: recursiveContext, srcPathStrings: []string{fullDir + "/"}, destPathString: pkg.TestMount() + "/fullDirExcludeDelete1", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "setup dir for exclude file delete", ctx: recursiveContext, srcPathStrings: []string{fullDir + "/"}, destPathString: pkg.TestMount() + "/fullDirExcludeDelete2", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "setup dir for exclude sym delete", ctx: recursiveContext, srcPathStrings: []string{fullDir + "/"}, destPathString: pkg.TestMount() + "/fullDirExcludeDelete3", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "setup dir for multi-level single delete", ctx: recursiveContext, srcPathStrings: []string{fullDirMulti + "/"}, destPathString: pkg.TestMount() + "/fullDirExcludeDelete4", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "setup dir for multi-level 2 path", ctx: recursiveContext, srcPathStrings: []string{fullDirMulti + "/"}, destPathString: pkg.TestMount() + "/fullDirExcludeDelete5", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "setup dir for multi-level 2 path match", ctx: recursiveContext, srcPathStrings: []string{fullDirMulti + "/"}, destPathString: pkg.TestMount() + "/fullDirExcludeDelete6", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "setup dir for multi-level 3 path", ctx: recursiveContext, srcPathStrings: []string{fullDirMulti + "/"}, destPathString: pkg.TestMount() + "/fullDirExcludeDelete7", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
		},
		{
			{name: "exclude path multi-level", ctx: setupContext(true, false, true, false, false, false, "exc_test_fil?.txt"), srcPathStrings: []string{fullDirMultiDel + "/"}, destPathString: pkg.TestMount() + "/fullDirExcludeDelete4", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: false,
				excluded: map[string]bool{pkg.TestMount() + "/fullDirExcludeDelete4/exc_test_file.txt": true, pkg.TestMount() + "/fullDirExcludeDelete4/.exc_test_file.txt.bedf03db267a01f0d1804dddfda43c6bf0520dd0": true, pkg.TestMount() + "/fullDirExcludeDelete4/exc_test_dir/exc_test_file.txt": true,
					pkg.TestMount() + "/fullDirExcludeDelete4/exc_test_dir/.exc_test_file.txt.30464bac355f9456f998a48dec3d7b8b24df779b": true, pkg.TestMount() + "/fullDirExcludeDelete4/exc_test_dir/exc_test_dir/exc_test_filt.txt": true,
					pkg.TestMount() + "/fullDirExcludeDelete4/exc_test_dir/exc_test_dir/.exc_test_filt.txt.9bf0179dd3794ba7cfc63c81e3d8405093851593": true}},
		},
		{
			{name: "exclude dir in delete", ctx: setupContext(true, false, true, false, false, false, "exc_test_dir"), srcPathStrings: []string{fullDir + "/exc_test_dir_empty/"}, destPathString: pkg.TestMount() + "/fullDirExcludeDelete1", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true,
				excluded: map[string]bool{pkg.TestMount() + "/fullDirExcludeDelete1/exc_test_dir": true, pkg.TestMount() + "/fullDirExcludeDelete1/exc_test_dir/exc_test_file.txt": true}}, // Not checking the dot file
			{name: "exclude file in delete", ctx: setupContext(true, false, true, false, false, false, "exc_test_fil?.txt"), srcPathStrings: []string{fullDir + "/exc_test_dir_empty/"}, destPathString: pkg.TestMount() + "/fullDirExcludeDelete2", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: false,
				excluded: map[string]bool{pkg.TestMount() + "/fullDirExcludeDelete2/exc_test_file.txt": true, pkg.TestMount() + "/fullDirExcludeDelete2/.exc_test_file.txt.813d9dc6ff6d74e90a960d3b9a5a811a557b2f78": true}},
			{name: "exclude sym in delete", ctx: setupContext(true, false, true, false, false, false, "exc_test_file_*"), srcPathStrings: []string{fullDir + "/exc_test_dir_empty/"}, destPathString: pkg.TestMount() + "/fullDirExcludeDelete3", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true,
				excluded: map[string]bool{pkg.TestMount() + "/fullDirExcludeDelete3/exc_test_file_sym.txt": true}},
			{name: "exclude path multi-level 2 path", ctx: setupContext(true, false, true, false, false, false, "exc_test_dir/exc_test_fil?.txt"), srcPathStrings: []string{fullDirMultiDel + "/"}, destPathString: pkg.TestMount() + "/fullDirExcludeDelete5", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: false,
				excluded: map[string]bool{pkg.TestMount() + "/fullDirExcludeDelete5/exc_test_dir/exc_test_file.txt": true, pkg.TestMount() + "/fullDirExcludeDelete5/exc_test_dir/.exc_test_file.txt.30464bac355f9456f998a48dec3d7b8b24df779b": true,
					pkg.TestMount() + "/fullDirExcludeDelete5/exc_test_dir/exc_test_dir/exc_test_filt.txt": true, pkg.TestMount() + "/fullDirExcludeDelete5/exc_test_dir/exc_test_dir/.exc_test_filt.txt.9bf0179dd3794ba7cfc63c81e3d8405093851593": true}},

			{name: "exclude path multi-level 2 path match", ctx: setupContext(true, false, true, false, false, false, "exc_test_*/exc_test_fil?.txt"), srcPathStrings: []string{fullDirMultiDel + "/"}, destPathString: pkg.TestMount() + "/fullDirExcludeDelete6", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: false,
				excluded: map[string]bool{pkg.TestMount() + "/fullDirExcludeDelete6/exc_test_dir/exc_test_file.txt": true, pkg.TestMount() + "/fullDirExcludeDelete6/exc_test_dir/.exc_test_file.txt.30464bac355f9456f998a48dec3d7b8b24df779b": true,
					pkg.TestMount() + "/fullDirExcludeDelete6/exc_test_dir/exc_test_dir/exc_test_filt.txt": true, pkg.TestMount() + "/fullDirExcludeDelete6/exc_test_dir/exc_test_dir/.exc_test_filt.txt.9bf0179dd3794ba7cfc63c81e3d8405093851593": true}},

			{name: "exclude path multi-level 3 path", ctx: setupContext(true, false, true, false, false, false, "exc_test_dir/exc_test_*/exc_test_fil?.txt"), srcPathStrings: []string{fullDirMultiDel + "/"}, destPathString: pkg.TestMount() + "/fullDirExcludeDelete7", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: false,
				excluded: map[string]bool{pkg.TestMount() + "/fullDirExcludeDelete7/exc_test_dir/exc_test_dir/exc_test_filt.txt": true, pkg.TestMount() + "/fullDirExcludeDelete7/exc_test_dir/exc_test_dir/.exc_test_filt.txt.9bf0179dd3794ba7cfc63c81e3d8405093851593": true}},
		},
	}
	if runTest {
		reinstateExternalFuncs := setupExternalFuncs()
		testRunner(t, tests)
		reinstateExternalFuncs()
	}
	return tests, teardownFileDirE2E
}

func TestE2EExcludePurge(t *testing.T) {
	E2EExcludePurgeHelper(t, true)
}

func E2EExcludePurgeHelper(t *testing.T, runTest bool) ([][]Test, func(t *testing.T)) {
	// t.Parallel()
	// setupEnvironmentE2E()
	teardownFileDirE2E, fullDir, fullDirMulti, fullDirMultiDel := setupExcludeDeleteTestE2E(t)
	if runTest {
		defer teardownFileDirE2E(t)
	}
	purgeExcludeContext := func(excludeStr string) Context {
		newCtx := setupContext(true, false, true, false, false, false, excludeStr)
		newCtx.purge = true
		return newCtx
	}
	// fullDirPath := pathlib.NewPath(fullDir)
	tests := [][]Test{
		{
			{name: "setup dir for exclude dir purge", ctx: recursiveContext, srcPathStrings: []string{fullDir + "/"}, destPathString: pkg.TestMount() + "/fullDirExcludePurge1", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "setup dir for exclude file purge", ctx: recursiveContext, srcPathStrings: []string{fullDir + "/"}, destPathString: pkg.TestMount() + "/fullDirExcludePurge2", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "setup dir for exclude sym purge", ctx: recursiveContext, srcPathStrings: []string{fullDir + "/"}, destPathString: pkg.TestMount() + "/fullDirExcludePurge3", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "setup dir for multi-level single purge", ctx: recursiveContext, srcPathStrings: []string{fullDirMulti + "/"}, destPathString: pkg.TestMount() + "/fullDirExcludePurge4", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "setup dir for multi-level 2 path purge", ctx: recursiveContext, srcPathStrings: []string{fullDirMulti + "/"}, destPathString: pkg.TestMount() + "/fullDirExcludePurge5", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "setup dir for multi-level 2 path match purge", ctx: recursiveContext, srcPathStrings: []string{fullDirMulti + "/"}, destPathString: pkg.TestMount() + "/fullDirExcludePurge6", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "setup dir for multi-level 3 path purge", ctx: recursiveContext, srcPathStrings: []string{fullDirMulti + "/"}, destPathString: pkg.TestMount() + "/fullDirExcludePurge7", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
		},
		{
			{name: "exclude path multi-level purge", ctx: purgeExcludeContext("exc_test_fil?.txt"), srcPathStrings: []string{fullDirMultiDel + "/"}, destPathString: pkg.TestMount() + "/fullDirExcludePurge4", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: false,
				excluded: map[string]bool{pkg.TestMount() + "/fullDirExcludePurge4/exc_test_file.txt": true, pkg.TestMount() + "/fullDirExcludePurge4/.exc_test_file.txt.bedf03db267a01f0d1804dddfda43c6bf0520dd0": true, pkg.TestMount() + "/fullDirExcludePurge4/exc_test_dir/exc_test_file.txt": true,
					pkg.TestMount() + "/fullDirExcludePurge4/exc_test_dir/.exc_test_file.txt.30464bac355f9456f998a48dec3d7b8b24df779b": true, pkg.TestMount() + "/fullDirExcludePurge4/exc_test_dir/exc_test_dir/exc_test_filt.txt": true,
					pkg.TestMount() + "/fullDirExcludePurge4/exc_test_dir/exc_test_dir/.exc_test_filt.txt.9bf0179dd3794ba7cfc63c81e3d8405093851593": true}},
		},
		{
			{name: "exclude dir in purge", ctx: purgeExcludeContext("exc_test_dir"), srcPathStrings: []string{fullDir + "/exc_test_dir_empty/"}, destPathString: pkg.TestMount() + "/fullDirExcludePurge1", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: false,
				excluded: map[string]bool{pkg.TestMount() + "/fullDirExcludePurge1/exc_test_dir": true, pkg.TestMount() + "/fullDirExcludePurge1/exc_test_dir/exc_test_file.txt": true}, purgeList: []string{"fullDirExcludePurge1/.exc_test_file.txt.813d9dc6ff6d74e90a960d3b9a5a811a557b2f78"}},
			{name: "exclude file in purge", ctx: purgeExcludeContext("exc_test_fil?.txt"), srcPathStrings: []string{fullDir + "/exc_test_dir_empty/"}, destPathString: pkg.TestMount() + "/fullDirExcludePurge2", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: false,
				excluded: map[string]bool{pkg.TestMount() + "/fullDirExcludePurge2/exc_test_file.txt": true, pkg.TestMount() + "/fullDirExcludePurge2/.exc_test_file.txt.813d9dc6ff6d74e90a960d3b9a5a811a557b2f78": true}, purgeList: []string{"fullDirExcludePurge2/exc_test_dir/.exc_test_file.txt.b794c8ec629e6d3e1056287dd576f077176fb3df"}},
			{name: "exclude sym in purge", ctx: purgeExcludeContext("exc_test_file_*"), srcPathStrings: []string{fullDir + "/exc_test_dir_empty/"}, destPathString: pkg.TestMount() + "/fullDirExcludePurge3", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: false,
				excluded: map[string]bool{pkg.TestMount() + "/fullDirExcludePurge3/exc_test_file_sym.txt": true}, purgeList: []string{"fullDirExcludePurge3/.exc_test_file.txt.813d9dc6ff6d74e90a960d3b9a5a811a557b2f78", "fullDirExcludePurge3/exc_test_dir/.exc_test_file.txt.b794c8ec629e6d3e1056287dd576f077176fb3df"}},
			{name: "exclude path multi-level 2 path purge", ctx: purgeExcludeContext("exc_test_dir/exc_test_fil?.txt"), srcPathStrings: []string{fullDirMultiDel + "/"}, destPathString: pkg.TestMount() + "/fullDirExcludePurge5", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: false,
				excluded: map[string]bool{pkg.TestMount() + "/fullDirExcludePurge5/exc_test_dir/exc_test_file.txt": true, pkg.TestMount() + "/fullDirExcludePurge5/exc_test_dir/.exc_test_file.txt.30464bac355f9456f998a48dec3d7b8b24df779b": true,
					pkg.TestMount() + "/fullDirExcludePurge5/exc_test_dir/exc_test_dir/exc_test_filt.txt": true, pkg.TestMount() + "/fullDirExcludePurge5/exc_test_dir/exc_test_dir/.exc_test_filt.txt.9bf0179dd3794ba7cfc63c81e3d8405093851593": true}, purgeList: []string{"fullDirExcludePurge5/.exc_test_file.txt.bedf03db267a01f0d1804dddfda43c6bf0520dd0"}},

			{name: "exclude path multi-level 2 path match purge", ctx: purgeExcludeContext("exc_test_*/exc_test_fil?.txt"), srcPathStrings: []string{fullDirMultiDel + "/"}, destPathString: pkg.TestMount() + "/fullDirExcludePurge6", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: false,
				excluded: map[string]bool{pkg.TestMount() + "/fullDirExcludePurge6/exc_test_dir/exc_test_file.txt": true, pkg.TestMount() + "/fullDirExcludePurge6/exc_test_dir/.exc_test_file.txt.30464bac355f9456f998a48dec3d7b8b24df779b": true,
					pkg.TestMount() + "/fullDirExcludePurge6/exc_test_dir/exc_test_dir/exc_test_filt.txt": true, pkg.TestMount() + "/fullDirExcludePurge6/exc_test_dir/exc_test_dir/.exc_test_filt.txt.9bf0179dd3794ba7cfc63c81e3d8405093851593": true}, purgeList: []string{"fullDirExcludePurge6/.exc_test_file.txt.bedf03db267a01f0d1804dddfda43c6bf0520dd0"}},

			{name: "exclude path multi-level 3 path purge", ctx: purgeExcludeContext("exc_test_dir/exc_test_*/exc_test_fil?.txt"), srcPathStrings: []string{fullDirMultiDel + "/"}, destPathString: pkg.TestMount() + "/fullDirExcludePurge7", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: false,
				excluded: map[string]bool{pkg.TestMount() + "/fullDirExcludePurge7/exc_test_dir/exc_test_dir/exc_test_filt.txt": true, pkg.TestMount() + "/fullDirExcludePurge7/exc_test_dir/exc_test_dir/.exc_test_filt.txt.9bf0179dd3794ba7cfc63c81e3d8405093851593": true}, purgeList: []string{"fullDirExcludePurge7/.exc_test_file.txt.bedf03db267a01f0d1804dddfda43c6bf0520dd0", "fullDirExcludePurge7/exc_test_dir/.exc_test_file.txt.30464bac355f9456f998a48dec3d7b8b24df779b"}},
		},
	}
	if runTest {
		reinstateExternalFuncs := setupExternalFuncs()
		testRunner(t, tests)
		reinstateExternalFuncs()
	}
	return tests, teardownFileDirE2E
}

func TestE2EPermissionChecking(t *testing.T) {
	E2EPermissionCheckingHelper(t, true)
}

func E2EPermissionCheckingHelper(t *testing.T, runTest bool) ([][]Test, func(t *testing.T)) {
	// t.Parallel()
	// setupEnvironmentE2E()
	addPermissionedDirs(defaultContext)
	teardownFileDirE2E, disallowUpload, disallowUploadAllow, allowUpload, allowUploadDisallow, disallowDelete,
		disallowDeleteAllow, allowDelete, emptyDir, differentFile, sym, emptyInnerDir := setupPermissionTestE2E(t)
	if runTest {
		defer teardownFileDirE2E(t)
	}
	// fullDirPath := pathlib.NewPath(fullDir)
	tests := [][]Test{
		{
			{name: "upload allowed in inner dir", ctx: recursiveContext, srcPathStrings: []string{disallowUploadAllow + "/"}, destPathString: pkg.TestMount() + "/disallow-upload", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "upload allowed in dir", ctx: recursiveContext, srcPathStrings: []string{allowUpload + "/"}, destPathString: pkg.TestMount() + "/allow-upload", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},

			{name: "setup dir", ctx: recursiveContext, srcPathStrings: []string{disallowUpload + "/"}, destPathString: pkg.TestMount() + "/disallow-delete", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "setup dir", ctx: recursiveContext, srcPathStrings: []string{disallowUpload + "/"}, destPathString: pkg.TestMount() + "/disallow-delete/allow-delete-inner", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "setup dir", ctx: recursiveContext, srcPathStrings: []string{disallowUpload + "/"}, destPathString: pkg.TestMount() + "/allow-delete", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "setup dir", ctx: recursiveContext, srcPathStrings: []string{disallowUpload + "/"}, destPathString: pkg.TestMount() + "/allow-delete/disallow-delete-inner", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "check permissions upload fail to dir", ctx: recursiveContext, srcPathStrings: []string{differentFile}, destPathString: pkg.TestMount() + "/check-permissions", copyOver: false, shouldError: true, noChange: true, successNoDotScheme: true},
			{name: "check permissions upload file fails (can't write to outer dir)", ctx: recursiveContext, srcPathStrings: []string{differentFile}, destPathString: pkg.TestMount() + "/check-permissions/rwx-file", copyOver: false, shouldError: true, noChange: true, successNoDotScheme: true},
			{name: "check permissions upload succeeds to dir", ctx: recursiveContext, srcPathStrings: []string{differentFile}, destPathString: pkg.TestMount() + "/check-permissions2", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "check permissions upload file fails to dir", ctx: recursiveContext, srcPathStrings: []string{differentFile}, destPathString: pkg.TestMount() + "/check-permissions2/inner-dir", copyOver: false, shouldError: true, noChange: true, successNoDotScheme: true},
			{name: "check permissions upload dir over fails to dir", ctx: recursiveContext, srcPathStrings: []string{emptyDir}, destPathString: pkg.TestMount() + "/check-permissions2/inner-dir/", copyOver: false, shouldError: true, noChange: true, successNoDotScheme: true},
			{name: "check permissions upload dir fails to dir", ctx: recursiveContext, srcPathStrings: []string{emptyDir}, destPathString: pkg.TestMount() + "/check-permissions2/inner-dir", copyOver: false, shouldError: true, noChange: true, successNoDotScheme: true},
			{name: "check permissions upload ghost fails to dir", ctx: recursiveContext, srcPathStrings: []string{emptyDir}, destPathString: pkg.TestMount() + "/check-permissions2/inner-dir/ghost", copyOver: false, shouldError: true, noChange: true, successNoDotScheme: true},
			{name: "check permissions upload ghost to ghost fails to dir", ctx: recursiveContext, srcPathStrings: []string{emptyDir}, destPathString: pkg.TestMount() + "/check-permissions2/inner-dir/ghost/ghost2", copyOver: false, shouldError: true, noChange: true, successNoDotScheme: true},
			{name: "check permissions upload sym fails to dir", ctx: recursiveContext, srcPathStrings: []string{sym}, destPathString: pkg.TestMount() + "/check-permissions2/inner-dir", copyOver: false, shouldError: true, noChange: true, successNoDotScheme: true},
			{name: "check permissions upload file succeeds (can write to outer dir)", ctx: recursiveContext, srcPathStrings: []string{differentFile}, destPathString: pkg.TestMount() + "/check-permissions2/ro-file", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "check permissions upload succeed file", ctx: recursiveContext, srcPathStrings: []string{differentFile}, destPathString: pkg.TestMount() + "/check-permissions2/writeable-file", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "no check permissions upload succeeds file", ctx: recursiveContext, srcPathStrings: []string{differentFile}, destPathString: pkg.TestMount() + "/no-check-permissions/ro-file", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "no check permissions upload succeed file", ctx: recursiveContext, srcPathStrings: []string{differentFile}, destPathString: pkg.TestMount() + "/no-check-permissions/writeable-file", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "no check permissions upload file succeeds to dir", ctx: recursiveContext, srcPathStrings: []string{differentFile}, destPathString: pkg.TestMount() + "/no-check-permissions/inner-dir", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "no check permissions upload dir succeeds to dir", ctx: recursiveContext, srcPathStrings: []string{emptyDir}, destPathString: pkg.TestMount() + "/no-check-permissions/inner-dir", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "no check permissions upload dir succeeds over dir", ctx: recursiveContext, srcPathStrings: []string{emptyDir}, destPathString: pkg.TestMount() + "/no-check-permissions/inner-dir2/", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "no check permissions upload sym succeeds to dir", ctx: recursiveContext, srcPathStrings: []string{sym}, destPathString: pkg.TestMount() + "/no-check-permissions/inner-dir", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},

			{name: "check permissions upload fail to dir acl", ctx: recursiveContext, srcPathStrings: []string{differentFile}, destPathString: pkg.TestMount() + "/check-permissions3", copyOver: false, shouldError: true, noChange: true, successNoDotScheme: true},
			{name: "check permissions upload file fails (can't write to outer dir) acl", ctx: recursiveContext, srcPathStrings: []string{differentFile}, destPathString: pkg.TestMount() + "/check-permissions3/rwx-file", copyOver: false, shouldError: true, noChange: true, successNoDotScheme: true},
			{name: "check permissions upload succeeds to dir acl", ctx: recursiveContext, srcPathStrings: []string{differentFile}, destPathString: pkg.TestMount() + "/check-permissions4", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "check permissions upload file fails to dir acl", ctx: recursiveContext, srcPathStrings: []string{differentFile}, destPathString: pkg.TestMount() + "/check-permissions4/inner-dir", copyOver: false, shouldError: true, noChange: true, successNoDotScheme: true},
			{name: "check permissions upload dir over fails to dir acl", ctx: recursiveContext, srcPathStrings: []string{emptyDir}, destPathString: pkg.TestMount() + "/check-permissions4/inner-dir/", copyOver: false, shouldError: true, noChange: true, successNoDotScheme: true},
			{name: "check permissions upload dir fails to dir acl", ctx: recursiveContext, srcPathStrings: []string{emptyDir}, destPathString: pkg.TestMount() + "/check-permissions4/inner-dir", copyOver: false, shouldError: true, noChange: true, successNoDotScheme: true},
			{name: "check permissions upload ghost fails to dir acl", ctx: recursiveContext, srcPathStrings: []string{emptyDir}, destPathString: pkg.TestMount() + "/check-permissions4/inner-dir/ghost", copyOver: false, shouldError: true, noChange: true, successNoDotScheme: true},
			{name: "check permissions upload ghost to ghost fails to dir acl", ctx: recursiveContext, srcPathStrings: []string{emptyDir}, destPathString: pkg.TestMount() + "/check-permissions4/inner-dir/ghost/ghost2", copyOver: false, shouldError: true, noChange: true, successNoDotScheme: true},
			{name: "check permissions upload sym fails to dir acl", ctx: recursiveContext, srcPathStrings: []string{sym}, destPathString: pkg.TestMount() + "/check-permissions4/inner-dir", copyOver: false, shouldError: true, noChange: true, successNoDotScheme: true},
			{name: "check permissions upload file succeeds (can write to outer dir) acl", ctx: recursiveContext, srcPathStrings: []string{differentFile}, destPathString: pkg.TestMount() + "/check-permissions4/ro-file", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "check permissions upload succeed file acl", ctx: recursiveContext, srcPathStrings: []string{differentFile}, destPathString: pkg.TestMount() + "/check-permissions4/writeable-file", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},

			{name: "acl flag allowed", ctx: aclRecursiveContext, srcPathStrings: []string{disallowUpload + "/"}, destPathString: pkg.TestMount() + "/acl-flag", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "acl flag not allowed", ctx: aclRecursiveContext, srcPathStrings: []string{disallowUpload + "/"}, destPathString: pkg.TestMount() + "/no-acl-flag", copyOver: false, shouldError: true, noChange: true, successNoDotScheme: true},
			{name: "acl flag not allowed inner", ctx: aclRecursiveContext, srcPathStrings: []string{disallowUpload + "/"}, destPathString: pkg.TestMount() + "/acl-flag/no-acl-flag", copyOver: false, shouldError: true, noChange: true, successNoDotScheme: true},
			{name: "acl flag not allowed, not used works", ctx: noAclRecursiveContext, srcPathStrings: []string{disallowUpload + "/"}, destPathString: pkg.TestMount() + "/no-acl-flag", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
		},
		{
			{name: "upload disallowed in dir", ctx: recursiveContext, srcPathStrings: []string{disallowUpload + "/"}, destPathString: pkg.TestMount() + "/disallow-upload", copyOver: false, shouldError: true, noChange: true, successNoDotScheme: true},
			{name: "upload disallowed in inner dir", ctx: recursiveContext, srcPathStrings: []string{allowUploadDisallow + "/"}, destPathString: pkg.TestMount() + "/allow-upload", copyOver: false, shouldError: true, noChange: true, successNoDotScheme: true},
			{name: "delete disallowed", ctx: deleteRecursiveContext, srcPathStrings: []string{disallowDelete + "/"}, destPathString: pkg.TestMount() + "/disallow-delete", copyOver: false, shouldError: true, noChange: true, successNoDotScheme: true},
			{name: "delete disallowed recursively", ctx: deleteRecursiveContext, srcPathStrings: []string{emptyInnerDir}, destPathString: pkg.TestMount() + "/disallow-delete/", copyOver: false, shouldError: true, noChange: true, successNoDotScheme: true},
			{name: "delete disallowed inner", ctx: deleteRecursiveContext, srcPathStrings: []string{allowDelete + "/"}, destPathString: pkg.TestMount() + "/allow-delete/disallow-delete-inner", copyOver: false, shouldError: true, noChange: true, successNoDotScheme: true}, // This is a special one because the outer test can and will delete the inner directory
		},
		{
			{name: "acl flag not allowed inner, not used works", ctx: noAclRecursiveContext, srcPathStrings: []string{disallowUpload + "/"}, destPathString: pkg.TestMount() + "/acl-flag/no-acl-flag", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "Can still delete even though can't upload", ctx: deleteRecursiveContext, srcPathStrings: []string{emptyDir + "/"}, destPathString: pkg.TestMount() + "/disallow-upload", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "delete allowed inner", ctx: deleteRecursiveContext, srcPathStrings: []string{disallowDeleteAllow + "/"}, destPathString: pkg.TestMount() + "/disallow-delete", copyOver: false, shouldError: true, noChange: false, successNoDotScheme: true},
			{name: "delete allowed", ctx: deleteRecursiveContext, srcPathStrings: []string{allowDelete + "/"}, destPathString: pkg.TestMount() + "/allow-delete", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
		},
		{
			{name: "Can still upload even though can't delete", ctx: recursiveContext, srcPathStrings: []string{disallowUpload + "/"}, destPathString: pkg.TestMount() + "/disallow-delete", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
		},
	}
	if runTest {
		reinstateExternalFuncs := setupExternalFuncs()
		testRunner(t, tests)
		reinstateExternalFuncs()
	}
	return tests, teardownFileDirE2E
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

func E2EPermissionDefaultCheckingHelper(t *testing.T, runTest bool) ([][]Test, func(t *testing.T)) {
	// t.Parallel()
	// setupEnvironmentE2E()
	teardownFileDirE2E, testFile := setupPermissionDefaultTestE2E(t)
	if runTest {
		defer teardownFileDirE2E(t)
	}
	// fullDirPath := pathlib.NewPath(fullDir)

	tests := [][]Test{
		{}, {}, {}, {}, {}, {},
		{
			{name: "Can't upload over ghost", ctx: defaultContext, srcPathStrings: []string{testFile}, destPathString: pkg.TestMount() + "/default_ghost/test_file.txt", copyOver: false, shouldError: true, noChange: true, successNoDotScheme: true},
			{name: "Can't upload", ctx: defaultContext, srcPathStrings: []string{testFile}, destPathString: pkg.TestMount(), copyOver: false, shouldError: true, noChange: true, successNoDotScheme: true},
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
	return tests, teardownFileDirE2E
}

func TestE2ERelative(t *testing.T) {
	E2ERelativeHelper(t, true)
}

func E2ERelativeHelper(t *testing.T, runTest bool) ([][]Test, func(t *testing.T)) {
	// t.Parallel()
	// setupEnvironmentE2E()
	t1teardownFileE2E, fullDir, innerDir, innerEmptyDir, innerFile, fullDir2, innerDir2, innerEmptyDir2, innerFile2 := setupRelativeTestE2E(t)
	if runTest {
		defer t1teardownFileE2E(t)
	}
	tests := [][]Test{
		{
			{name: "2 relative files", ctx: relativeContext, srcPathStrings: []string{innerFile, innerFile2}, destPathString: pkg.TestMount(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "2 relative files over ghost", ctx: relativeContext, srcPathStrings: []string{innerFile, innerFile2}, destPathString: pkg.TestMount() + "/relativeGhost", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
		},
		{
			{name: "2 relative files over same", ctx: relativeContext, srcPathStrings: []string{innerFile, innerFile2}, destPathString: pkg.TestMount(), copyOver: false, shouldError: false, noChange: true, successNoDotScheme: true},
		},
		{
			{name: "2 relative dirs", ctx: relativeRecursiveContext, srcPathStrings: []string{innerDir, innerDir2}, destPathString: pkg.TestMount(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "2 relative dirs empty", ctx: relativeRecursiveContext, srcPathStrings: []string{innerEmptyDir, innerEmptyDir2}, destPathString: pkg.TestMount(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
		},
		{
			{name: "relative full dir", ctx: relativeRecursiveContext, srcPathStrings: []string{fullDir}, destPathString: pkg.TestMount(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "relative full dir over same (updates dir due to default perms when copying in)", ctx: relativeRecursiveContext, srcPathStrings: []string{fullDir2}, destPathString: pkg.TestMount(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
		},
		{
			{name: "relative full dir over same (compare)", ctx: relativeRecursiveContext, srcPathStrings: []string{fullDir2}, destPathString: pkg.TestMount(), copyOver: false, shouldError: false, noChange: true, successNoDotScheme: true},
		},
	}
	if runTest {
		reinstateExternalFuncs := setupExternalFuncs()
		testRunner(t, tests)
		reinstateExternalFuncs()
	}
	return tests, t1teardownFileE2E
}

func TestE2EDirsFlag(t *testing.T) {
	E2EDirsFlagHelper(t, true)
}

func E2EDirsFlagHelper(t *testing.T, runTest bool) ([][]Test, func(t *testing.T)) {
	// t.Parallel()
	// setupEnvironmentE2E()
	teardownFileDirE2E, fullDir, testFile, testSym, _, _, fullDirHoldEmpty := setupDirsFlagTestE2E(t)
	if runTest {
		defer teardownFileDirE2E(t)
	}

	testFilePath := pathlib.NewPath(testFile)
	testSymPath := pathlib.NewPath(testSym)

	tests := [][]Test{
		{
			{name: "file, sym, dir in setup", ctx: recursiveContext, srcPathStrings: []string{testFile, testSym, fullDir}, destPathString: pkg.TestMount() + "/dirs_dirs_over", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "file, sym, dir in setup", ctx: recursiveContext, srcPathStrings: []string{testFile, testSym, fullDir}, destPathString: pkg.TestMount() + "/dirs_dirs_del", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "full dir non-dirs (error)", ctx: defaultContext, srcPathStrings: []string{fullDir}, destPathString: pkg.TestMount(), copyOver: false, shouldError: true, noChange: false, successNoDotScheme: true},
			{name: "full dir dirs", ctx: dirsContext, srcPathStrings: []string{fullDir}, destPathString: pkg.TestMount(), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "full dir to ghost copies in", ctx: dirsContext, srcPathStrings: []string{fullDir}, destPathString: pkg.TestMount() + "/dirs_ghost_dir", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "full dir to ghost/ copies in", ctx: dirsContext, srcPathStrings: []string{fullDir}, destPathString: pkg.TestMount() + "/dirs_ghost_dir2/", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "full dir/ to ghost copies in", ctx: dirsContext, srcPathStrings: []string{fullDir + "/"}, destPathString: pkg.TestMount() + "/dirs_ghost_dir3", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
		},
		{
			{name: "full dir dirs over same", ctx: dirsContext, srcPathStrings: []string{fullDir}, destPathString: pkg.TestMount(), copyOver: false, shouldError: false, noChange: true, successNoDotScheme: true},
		},
		{
			{name: "dirs over file", ctx: dirsContext, srcPathStrings: []string{fullDir}, destPathString: pkg.TestMount() + "/dirs_dirs_over/" + testFilePath.Name(), copyOver: true, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "dirs over sym", ctx: dirsContext, srcPathStrings: []string{fullDir}, destPathString: pkg.TestMount() + "/dirs_dirs_over/" + testSymPath.Name(), copyOver: true, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "dirs delete not recursive", ctx: dirsDeleteContext, srcPathStrings: []string{fullDirHoldEmpty + "/"}, destPathString: pkg.TestMount() + "/dirs_dirs_del", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
		},
	}
	if runTest {
		reinstateExternalFuncs := setupExternalFuncs()
		testRunner(t, tests)
		reinstateExternalFuncs()
	}
	return tests, teardownFileDirE2E
}

func TestE2EPurgeDirsFlag(t *testing.T) {
	E2EPurgeDirsFlagHelper(t, true)
}

func E2EPurgeDirsFlagHelper(t *testing.T, runTest bool) ([][]Test, func(t *testing.T)) {
	// t.Parallel()
	// setupEnvironmentE2E()
	teardownFileDirE2E, fullDir, testFile, testSym, innerDir, innerTestFile, fullDirHoldEmpty := setupDirsFlagTestE2E(t)
	if runTest {
		defer teardownFileDirE2E(t)
	}

	fullDirPath := pathlib.NewPath(fullDir)
	testFilePath := pathlib.NewPath(testFile)
	testSymPath := pathlib.NewPath(testSym)
	innerDirPath := pathlib.NewPath(innerDir)
	innerTestFilePath := pathlib.NewPath(innerTestFile)
	dirsFilePurge := func() []string {
		if recursiveContext.cfg.Repo.DotScheme {
			return []string{}
		} else {
			return []string{"dirs_dirs_over_purge/" + testFilePath.Name()}
		}
	}
	stdPurgeList := func(prefix string) []string {
		if recursiveContext.cfg.Repo.DotScheme {
			return []string{prefix + "/." + testFilePath.Name() + ".f24d7b797432a7aaf05c29e032faa297277a14f8", prefix + "/" + fullDirPath.Name() + "/." + testFilePath.Name() + ".f24d7b797432a7aaf05c29e032faa297277a14f8", prefix + "/" + fullDirPath.Name() + "/" + innerDirPath.Name() + "/." + innerTestFilePath.Name() + ".c88edb05fad9a291d3c26786291f4b18d74c76a0"}
		} else {
			return []string{prefix + "/" + testFilePath.Name(), prefix + "/" + fullDirPath.Name() + "/" + testFilePath.Name(), prefix + "/" + fullDirPath.Name() + "/" + innerDirPath.Name() + "/" + innerTestFilePath.Name()}
		}
	}
	tests := [][]Test{
		{
			{name: "file, sym, dir in setup purge", ctx: recursiveContext, srcPathStrings: []string{testFile, testSym, fullDir}, destPathString: pkg.TestMount() + "/dirs_dirs_over_purge", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
			{name: "file, sym, dir in setup purge", ctx: recursiveContext, srcPathStrings: []string{testFile, testSym, fullDir}, destPathString: pkg.TestMount() + "/dirs_dirs_del_purge", copyOver: false, shouldError: false, noChange: false, successNoDotScheme: true},
		},
		{},
		{
			{name: "dirs over file purge", ctx: dirsDeletePurgeContext, srcPathStrings: []string{fullDir}, destPathString: pkg.TestMount() + "/dirs_dirs_over_purge/" + testFilePath.Name(), purgeList: dirsFilePurge(), copyOver: true, shouldError: false, noChange: false, successNoDotScheme: false},
			{name: "dirs over sym purge", ctx: dirsDeletePurgeContext, srcPathStrings: []string{fullDir}, destPathString: pkg.TestMount() + "/dirs_dirs_over_purge/" + testSymPath.Name(), copyOver: true, shouldError: false, noChange: false, successNoDotScheme: false},
			{name: "dirs delete not recursive purge", ctx: dirsDeletePurgeContext, srcPathStrings: []string{fullDirHoldEmpty + "/"}, destPathString: pkg.TestMount() + "/dirs_dirs_del_purge", purgeList: stdPurgeList("dirs_dirs_del_purge"), copyOver: false, shouldError: false, noChange: false, successNoDotScheme: false},
		},
	}
	if runTest {
		reinstateExternalFuncs := setupExternalFuncs()
		testRunner(t, tests)
		reinstateExternalFuncs()
	}
	return tests, teardownFileDirE2E
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
