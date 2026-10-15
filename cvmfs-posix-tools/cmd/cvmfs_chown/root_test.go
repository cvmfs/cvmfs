package main

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	pathlib "github.com/chigopher/pathlib"
	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
)

type Test struct {
	name        string
	ctx         Context
	expecPOG    []ExpectedPathOwnerGroup
	ownerString string
	paths       []string
	shouldError bool
}

func setupContext(recursive bool, referenceStr string) Context {
	ctx := Context{
		recursive:    recursive,
		reference:    referenceStr,
		referenceSet: referenceStr != "",
		debug:        true,
		numHashers:   30,
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

func fastTestRunner(t *testing.T, tests [][]Test) {
	for _, tList := range tests {
		for _, tc := range tList {
			fmt.Printf("\nRunning test %s\n", tc.name)
			graft = Mock_check_expected_graft_getter(tc.ctx, tc.expecPOG)
			paths := []*pathlib.Path{}
			for _, pathString := range tc.paths {
				paths = append(paths, pathlib.NewPath(pathString))
			}
			err := launchChown(tc.ctx, tc.ownerString, paths)
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

func TestE2EChown(t *testing.T) {
	E2EChownHelper(t, true)
}

func E2EChownHelper(t *testing.T, runTest bool) ([][]Test, func(t *testing.T), pkg.FileHashData) {
	var defaultContext = setupContext(false, "")
	var recursiveContext = setupContext(true, "")
	tearDown, refFile, fHashData := setupTestEnv(defaultContext)
	if runTest {
		defer tearDown(t)
	}
	uid := os.Getuid()
	gid := os.Getegid()
	tests := [][]Test{
		{
			{name: "Chown of file, malformed", ctx: defaultContext, ownerString: "Not a user", paths: []string{pkg.TestMount() + "/file.txt"}, shouldError: true},
			{name: "Chown of file, no own", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount() + "/check-permissions/no_perm.txt"}, shouldError: true},
			{name: "Chown of dir, no own", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount() + "/check-permissions/no_own_dir"}, shouldError: true},
			{name: "Chown of sym -> file, no own", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount() + "/check-permissions/no_perm2_sym.txt"}, shouldError: true},
			{name: "Chown of sym -> dir, no own", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount() + "/check-permissions/no_own_dir2_sym"}, shouldError: true},

			{name: "Chown of file, no own, no perm check", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount() + "/no_perm.txt"}, shouldError: false},
			{name: "Chown of dir, no own, no perm check", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount() + "/no_own_dir"}, shouldError: false},
			{name: "Chown of sym -> file, no own, no perm check", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount() + "/no_perm2_sym.txt"}, shouldError: false},
			{name: "Chown of sym -> dir, no own, no perm check", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount() + "/no_own_dir2_sym"}, shouldError: false},

			{name: "Chown of file, name", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount() + "/file.txt"}, shouldError: false,
				expecPOG: []ExpectedPathOwnerGroup{{sym: true, path: "file.txt", owner: pkg.TestingUnownedUGid, group: gid, noDotSchemeFile: true}, {file: true, path: ".file.txt." + fmt.Sprintf("%040x", fHashData.Checksum), owner: pkg.TestingUnownedUGid, group: gid, noDotSchemeAbsent: true}}},
			{name: "Chown of dir, name", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount() + "/dir"}, shouldError: false,
				expecPOG: []ExpectedPathOwnerGroup{{dir: true, path: "dir", owner: pkg.TestingUnownedUGid, group: gid}}},
			{name: "Chown of sym, name", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount() + "/sym_file.txt"}, shouldError: false,
				expecPOG: []ExpectedPathOwnerGroup{{sym: true, path: "sym_file.txt", owner: pkg.TestingUnownedUGid, group: gid}}},
			{name: "Chown of file, id", ctx: defaultContext, ownerString: strconv.Itoa(pkg.TestingUnownedUGid), paths: []string{pkg.TestMount() + "/file2.txt"}, shouldError: false,
				expecPOG: []ExpectedPathOwnerGroup{{sym: true, path: "file2.txt", owner: pkg.TestingUnownedUGid, group: gid, noDotSchemeFile: true}, {file: true, path: ".file2.txt." + fmt.Sprintf("%040x", fHashData.Checksum), owner: pkg.TestingUnownedUGid, group: gid, noDotSchemeAbsent: true}}},
			{name: "Chown of multiple files", ctx: defaultContext, ownerString: strconv.Itoa(pkg.TestingUnownedUGid), paths: []string{pkg.TestMount() + "/dir/file.txt", pkg.TestMount() + "/dir/file2.txt"}, shouldError: false,
				expecPOG: []ExpectedPathOwnerGroup{{sym: true, path: "dir/file.txt", owner: pkg.TestingUnownedUGid, group: gid, noDotSchemeFile: true}, {file: true, path: "dir/.file.txt." + fmt.Sprintf("%040x", fHashData.Checksum), owner: pkg.TestingUnownedUGid, group: gid, noDotSchemeAbsent: true},
					{sym: true, path: "dir/file2.txt", owner: pkg.TestingUnownedUGid, group: gid, noDotSchemeFile: true}, {file: true, path: "dir/.file2.txt." + fmt.Sprintf("%040x", fHashData.Checksum), owner: pkg.TestingUnownedUGid, group: gid, noDotSchemeAbsent: true}}},
			{name: "Chown group of file, name", ctx: defaultContext, ownerString: fmt.Sprintf("%s:%s", pkg.TestingUnownedUserGroup, pkg.TestingUnownedUserGroup), paths: []string{pkg.TestMount() + "/dir/file.txt"}, shouldError: false,
				expecPOG: []ExpectedPathOwnerGroup{{sym: true, path: "dir/file.txt", owner: pkg.TestingUnownedUGid, group: pkg.TestingUnownedUGid, noDotSchemeFile: true}, {file: true, path: "dir/.file.txt." + fmt.Sprintf("%040x", fHashData.Checksum), owner: pkg.TestingUnownedUGid, group: pkg.TestingUnownedUGid, noDotSchemeAbsent: true}}},
			{name: "Chown group of dir, name", ctx: defaultContext, ownerString: fmt.Sprintf("%s:%s", pkg.TestingUnownedUserGroup, pkg.TestingUnownedUserGroup), paths: []string{pkg.TestMount() + "/dir/inner_dir"}, shouldError: false,
				expecPOG: []ExpectedPathOwnerGroup{{dir: true, path: "dir/inner_dir", owner: pkg.TestingUnownedUGid, group: pkg.TestingUnownedUGid}}},
			{name: "Chown group of sym, name", ctx: defaultContext, ownerString: fmt.Sprintf("%s:%s", pkg.TestingUnownedUserGroup, pkg.TestingUnownedUserGroup), paths: []string{pkg.TestMount() + "/dir/sym_file.txt"}, shouldError: false,
				expecPOG: []ExpectedPathOwnerGroup{{sym: true, path: "dir/sym_file.txt", owner: pkg.TestingUnownedUGid, group: pkg.TestingUnownedUGid}}},
			{name: "Chown group of file, id", ctx: defaultContext, ownerString: fmt.Sprintf("%d:%d", pkg.TestingUnownedUGid, pkg.TestingUnownedUGid), paths: []string{pkg.TestMount() + "/dir/file2.txt"}, shouldError: false,
				expecPOG: []ExpectedPathOwnerGroup{{sym: true, path: "dir/file2.txt", owner: pkg.TestingUnownedUGid, group: pkg.TestingUnownedUGid, noDotSchemeFile: true}, {file: true, path: "dir/.file2.txt." + fmt.Sprintf("%040x", fHashData.Checksum), owner: pkg.TestingUnownedUGid, group: pkg.TestingUnownedUGid, noDotSchemeAbsent: true}}},
			{name: "Chown group too many colons", ctx: defaultContext, ownerString: "0:0:0", paths: []string{pkg.TestMount() + "/file3.txt"}, shouldError: true},

			// FAIL: SHOULD MAYBE NOT DO ANYTHING, OTHERWISE GOOD
			// {name: "Chown of file/, (error)", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount()+ "/dir/inner_dir3/file.txt/"}, shouldError: true},
			// {name: "Chown of sym/, (error)", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount()+ "/dir/inner_dir3/sym_file.txt/"}, shouldError: true},

			// FAIL: SHOULD NOT DO ANYTHING OR CHANGE FILE/SYM
			// {name: "Chown of file/., (error)", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount()+ "/dir/inner_dir3/file.txt/."}, shouldError: true},
			// {name: "Chown of sym/., (error)", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount()+ "/dir/inner_dir3/sym_file.txt/."}, shouldError: true},

			// FAIL: SHOULD NOT DO ANYTHING OR CHANGE LEVEL UP
			// {name: "Chown of file/.., (error)", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount()+ "/dir/inner_dir3/file.txt/.."}, shouldError: true},
			// {name: "Chown of sym/.., (error)", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount()+ "/dir/inner_dir3/sym_file.txt/.."}, shouldError: true},

			{name: "Chown of dir/, change dir", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount() + "/dir/inner_dir3/inner_dir/"}, shouldError: false,
				expecPOG: []ExpectedPathOwnerGroup{{dir: true, path: "dir/inner_dir3/inner_dir", owner: pkg.TestingUnownedUGid, group: gid}}},

			{name: "Chown of dir/., change dir", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount() + "/dir/inner_dir3.1/inner_dir/."}, shouldError: false,
				expecPOG: []ExpectedPathOwnerGroup{{dir: true, path: "dir/inner_dir3.1/inner_dir", owner: pkg.TestingUnownedUGid, group: gid}}},
			{name: "Chown of dir/.., change dir", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount() + "/dir/inner_dir3.2/inner_dir/.."}, shouldError: false,
				expecPOG: []ExpectedPathOwnerGroup{{dir: true, path: "dir/inner_dir3.2", owner: pkg.TestingUnownedUGid, group: gid}}},
		},
		{
			{name: "Chown of file, reference", ctx: setupContext(false, refFile), ownerString: "", paths: []string{pkg.TestMount() + "/file4.txt"}, shouldError: false,
				expecPOG: []ExpectedPathOwnerGroup{{sym: true, path: "file4.txt", owner: uid, group: gid, noDotSchemeFile: true}, {file: true, path: ".file4.txt." + fmt.Sprintf("%040x", fHashData.Checksum), owner: uid, group: gid, noDotSchemeAbsent: true}}},
			{name: "Chown of dir recursive", ctx: recursiveContext, ownerString: strconv.Itoa(pkg.TestingUnownedUGid), paths: []string{pkg.TestMount() + "/dir/inner_dir2"}, shouldError: false,
				expecPOG: []ExpectedPathOwnerGroup{{sym: true, path: "dir/inner_dir2/file.txt", owner: pkg.TestingUnownedUGid, group: gid, noDotSchemeFile: true}, {file: true, path: "dir/inner_dir2/.file.txt." + fmt.Sprintf("%040x", fHashData.Checksum), owner: pkg.TestingUnownedUGid, group: gid, noDotSchemeAbsent: true},
					{sym: true, path: "dir/inner_dir2/sym_file.txt", owner: pkg.TestingUnownedUGid, group: gid}, {dir: true, path: "dir/inner_dir2", owner: pkg.TestingUnownedUGid, group: gid}}},

			{name: "Chown of cvmfs catalog, (error)", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount() + "/dir/.cvmfscatalog"}, shouldError: true},
			{name: "Chown of dir recursive /.", ctx: recursiveContext, ownerString: strconv.Itoa(pkg.TestingUnownedUGid), paths: []string{pkg.TestMount() + "/dir/inner_dir4/."}, shouldError: false,
				expecPOG: []ExpectedPathOwnerGroup{{sym: true, path: "dir/inner_dir4/file.txt", owner: pkg.TestingUnownedUGid, group: gid, noDotSchemeFile: true}, {file: true, path: "dir/inner_dir4/.file.txt." + fmt.Sprintf("%040x", fHashData.Checksum), owner: pkg.TestingUnownedUGid, group: gid, noDotSchemeAbsent: true},
					{sym: true, path: "dir/inner_dir4/sym_file.txt", owner: pkg.TestingUnownedUGid, group: gid}, {dir: true, path: "dir/inner_dir4", owner: pkg.TestingUnownedUGid, group: gid}}},

			{name: "Chown of dir recursive /..", ctx: recursiveContext, ownerString: strconv.Itoa(pkg.TestingUnownedUGid), paths: []string{pkg.TestMount() + "/dir/inner_dir5/inner_dir/.."}, shouldError: false,
				expecPOG: []ExpectedPathOwnerGroup{{sym: true, path: "dir/inner_dir5/file.txt", owner: pkg.TestingUnownedUGid, group: gid, noDotSchemeFile: true}, {file: true, path: "dir/inner_dir5/.file.txt." + fmt.Sprintf("%040x", fHashData.Checksum), owner: pkg.TestingUnownedUGid, group: gid, noDotSchemeAbsent: true},
					{sym: true, path: "dir/inner_dir5/sym_file.txt", owner: pkg.TestingUnownedUGid, group: gid}, {dir: true, path: "dir/inner_dir5", owner: pkg.TestingUnownedUGid, group: gid}, {dir: true, path: "dir/inner_dir5/inner_dir", owner: pkg.TestingUnownedUGid, group: gid}}},
			{name: "Chown of dir recursive /", ctx: recursiveContext, ownerString: strconv.Itoa(pkg.TestingUnownedUGid), paths: []string{pkg.TestMount() + "/dir/inner_dir6/"}, shouldError: false,
				expecPOG: []ExpectedPathOwnerGroup{{sym: true, path: "dir/inner_dir6/file.txt", owner: pkg.TestingUnownedUGid, group: gid, noDotSchemeFile: true}, {file: true, path: "dir/inner_dir6/.file.txt." + fmt.Sprintf("%040x", fHashData.Checksum), owner: pkg.TestingUnownedUGid, group: gid, noDotSchemeAbsent: true},
					{sym: true, path: "dir/inner_dir6/sym_file.txt", owner: pkg.TestingUnownedUGid, group: gid}, {dir: true, path: "dir/inner_dir6", owner: pkg.TestingUnownedUGid, group: gid}}},
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
