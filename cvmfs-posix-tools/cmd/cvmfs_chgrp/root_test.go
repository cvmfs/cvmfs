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
	expecPOG    []ExpectedPathGroup
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
			err := launchChgrp(tc.ctx, tc.ownerString, paths)
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

func TestE2EChgrp(t *testing.T) {
	E2EChgrpHelper(t, true)
}

func E2EChgrpHelper(t *testing.T, runTest bool) ([][]Test, func(t *testing.T), pkg.FileHashData) {
	defaultContext := setupContext(false, "")
	recursiveContext := setupContext(true, "")
	tearDown, refFile, fHashData := setupTestEnv(defaultContext)
	if runTest {
		defer tearDown(t)
	}
	gid := os.Getegid()
	tests := [][]Test{
		{
			{name: "Chgrp of file, malformed", ctx: defaultContext, ownerString: "Not a user", paths: []string{pkg.TestMount() + "/file.txt"}, shouldError: true},
			{name: "Chgrp of file, no own", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount() + "/check-permissions/no_perm.txt"}, shouldError: true},
			{name: "Chgrp of dir, no own", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount() + "/check-permissions/no_own_dir"}, shouldError: true},
			{name: "Chgrp of sym -> file, no own", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount() + "/check-permissions/no_perm2_sym.txt"}, shouldError: true},
			{name: "Chgrp of sym -> dir, no own", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount() + "/check-permissions/no_own_dir2_sym"}, shouldError: true},

			{name: "Chgrp of file, no own, no perm check", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount() + "/no_perm.txt"}, shouldError: false},
			{name: "Chgrp of dir, no own, no perm check", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount() + "/no_own_dir"}, shouldError: false},
			{name: "Chgrp of sym -> file, no own, no perm check", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount() + "/no_perm2_sym.txt"}, shouldError: false},
			{name: "Chgrp of sym -> dir, no own, no perm check", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount() + "/no_own_dir2_sym"}, shouldError: false},

			{name: "Chgrp of file, name", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount() + "/file.txt"}, shouldError: false,
				expecPOG: []ExpectedPathGroup{{sym: true, path: "file.txt", group: pkg.TestingUnownedUGid, noDotSchemeFile: true}, {file: true, path: ".file.txt." + fmt.Sprintf("%040x", fHashData.Checksum), group: pkg.TestingUnownedUGid, noDotSchemeAbsent: true}}},
			{name: "Chgrp of dir, name", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount() + "/dir"}, shouldError: false,
				expecPOG: []ExpectedPathGroup{{dir: true, path: "dir", group: pkg.TestingUnownedUGid}}},
			{name: "Chgrp of sym, name", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount() + "/sym_file.txt"}, shouldError: false,
				expecPOG: []ExpectedPathGroup{{sym: true, path: "sym_file.txt", group: pkg.TestingUnownedUGid}}},
			{name: "Chgrp of file, id", ctx: defaultContext, ownerString: strconv.Itoa(pkg.TestingUnownedUGid), paths: []string{pkg.TestMount() + "/file2.txt"}, shouldError: false,
				expecPOG: []ExpectedPathGroup{{sym: true, path: "file2.txt", group: pkg.TestingUnownedUGid, noDotSchemeFile: true}, {file: true, path: ".file2.txt." + fmt.Sprintf("%040x", fHashData.Checksum), group: pkg.TestingUnownedUGid, noDotSchemeAbsent: true}}},
			{name: "Chgrp of multiple files", ctx: defaultContext, ownerString: strconv.Itoa(pkg.TestingUnownedUGid), paths: []string{pkg.TestMount() + "/dir/file.txt", pkg.TestMount() + "/dir/file2.txt"}, shouldError: false,
				expecPOG: []ExpectedPathGroup{{sym: true, path: "dir/file.txt", group: pkg.TestingUnownedUGid, noDotSchemeFile: true}, {file: true, path: "dir/.file.txt." + fmt.Sprintf("%040x", fHashData.Checksum), group: pkg.TestingUnownedUGid, noDotSchemeAbsent: true},
					{sym: true, path: "dir/file2.txt", group: pkg.TestingUnownedUGid, noDotSchemeFile: true}, {file: true, path: "dir/.file2.txt." + fmt.Sprintf("%040x", fHashData.Checksum), group: pkg.TestingUnownedUGid, noDotSchemeAbsent: true}}},

			// FAIL: SHOULD MAYBE NOT DO ANYTHING, OTHERWISE GOOD
			// {name: "Chgrp of file/, (error)", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount() + "/dir/inner_dir3/file.txt/"}, shouldError: true},
			// {name: "Chgrp of sym/, (error)", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount() + "/dir/inner_dir3/sym_file.txt/"}, shouldError: true},

			// FAIL: SHOULD NOT DO ANYTHING OR CHANGE FILE/SYM
			// {name: "Chgrp of file/., (error)", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount() + "/dir/inner_dir3/file.txt/."}, shouldError: true},
			// {name: "Chgrp of sym/., (error)", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount() + "/dir/inner_dir3/sym_file.txt/."}, shouldError: true},

			// FAIL: SHOULD NOT DO ANYTHING OR CHANGE LEVEL UP
			// {name: "Chgrp of file/.., (error)", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount() + "/dir/inner_dir3/file.txt/.."}, shouldError: true},
			// {name: "Chgrp of sym/.., (error)", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount() + "/dir/inner_dir3/sym_file.txt/.."}, shouldError: true},

			{name: "Chgrp of dir/, change dir", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount() + "/dir/inner_dir3/inner_dir/"}, shouldError: false,
				expecPOG: []ExpectedPathGroup{{dir: true, path: "dir/inner_dir3/inner_dir", group: pkg.TestingUnownedUGid}}},

			{name: "Chgrp of dir/., change dir", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount() + "/dir/inner_dir3.1/inner_dir/."}, shouldError: false,
				expecPOG: []ExpectedPathGroup{{dir: true, path: "dir/inner_dir3.1/inner_dir", group: pkg.TestingUnownedUGid}}},
			{name: "Chgrp of dir/.., change dir", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount() + "/dir/inner_dir3.2/inner_dir/.."}, shouldError: false,
				expecPOG: []ExpectedPathGroup{{dir: true, path: "dir/inner_dir3.2", group: pkg.TestingUnownedUGid}}},
		},
		{
			{name: "Chgrp of file, reference", ctx: setupContext(false, refFile), ownerString: "", paths: []string{pkg.TestMount() + "/file4.txt"}, shouldError: false,
				expecPOG: []ExpectedPathGroup{{sym: true, path: "file4.txt", group: gid, noDotSchemeFile: true}, {file: true, path: ".file4.txt." + fmt.Sprintf("%040x", fHashData.Checksum), group: gid, noDotSchemeAbsent: true}}},
			{name: "Chgrp of dir recursive", ctx: recursiveContext, ownerString: strconv.Itoa(pkg.TestingUnownedUGid), paths: []string{pkg.TestMount() + "/dir/inner_dir2"}, shouldError: false,
				expecPOG: []ExpectedPathGroup{{sym: true, path: "dir/inner_dir2/file.txt", group: pkg.TestingUnownedUGid, noDotSchemeFile: true}, {file: true, path: "dir/inner_dir2/.file.txt." + fmt.Sprintf("%040x", fHashData.Checksum), group: pkg.TestingUnownedUGid, noDotSchemeAbsent: true},
					{sym: true, path: "dir/inner_dir2/sym_file.txt", group: pkg.TestingUnownedUGid}, {dir: true, path: "dir/inner_dir2", group: pkg.TestingUnownedUGid}}},
			{name: "Chgrp of cvmfs catalog, (error)", ctx: defaultContext, ownerString: pkg.TestingUnownedUserGroup, paths: []string{pkg.TestMount() + "/dir/.cvmfscatalog"}, shouldError: true},
			{name: "Chgrp of dir recursive /.", ctx: recursiveContext, ownerString: strconv.Itoa(pkg.TestingUnownedUGid), paths: []string{pkg.TestMount() + "/dir/inner_dir4/."}, shouldError: false,
				expecPOG: []ExpectedPathGroup{{sym: true, path: "dir/inner_dir4/file.txt", group: pkg.TestingUnownedUGid, noDotSchemeFile: true}, {file: true, path: "dir/inner_dir4/.file.txt." + fmt.Sprintf("%040x", fHashData.Checksum), group: pkg.TestingUnownedUGid, noDotSchemeAbsent: true},
					{sym: true, path: "dir/inner_dir4/sym_file.txt", group: pkg.TestingUnownedUGid}, {dir: true, path: "dir/inner_dir4", group: pkg.TestingUnownedUGid}}},

			{name: "Chgrp of dir recursive /..", ctx: recursiveContext, ownerString: strconv.Itoa(pkg.TestingUnownedUGid), paths: []string{pkg.TestMount() + "/dir/inner_dir5/inner_dir/.."}, shouldError: false,
				expecPOG: []ExpectedPathGroup{{sym: true, path: "dir/inner_dir5/file.txt", group: pkg.TestingUnownedUGid, noDotSchemeFile: true}, {file: true, path: "dir/inner_dir5/.file.txt." + fmt.Sprintf("%040x", fHashData.Checksum), group: pkg.TestingUnownedUGid, noDotSchemeAbsent: true},
					{sym: true, path: "dir/inner_dir5/sym_file.txt", group: pkg.TestingUnownedUGid}, {dir: true, path: "dir/inner_dir5", group: pkg.TestingUnownedUGid}, {dir: true, path: "dir/inner_dir5/inner_dir", group: pkg.TestingUnownedUGid}}},
			{name: "Chgrp of dir recursive /", ctx: recursiveContext, ownerString: strconv.Itoa(pkg.TestingUnownedUGid), paths: []string{pkg.TestMount() + "/dir/inner_dir6/"}, shouldError: false,
				expecPOG: []ExpectedPathGroup{{sym: true, path: "dir/inner_dir6/file.txt", group: pkg.TestingUnownedUGid, noDotSchemeFile: true}, {file: true, path: "dir/inner_dir6/.file.txt." + fmt.Sprintf("%040x", fHashData.Checksum), group: pkg.TestingUnownedUGid, noDotSchemeAbsent: true},
					{sym: true, path: "dir/inner_dir6/sym_file.txt", group: pkg.TestingUnownedUGid}, {dir: true, path: "dir/inner_dir6", group: pkg.TestingUnownedUGid}}},
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
	defer os.Remove(confFile)
	OVERRIDE_CONFIG_FLAG_SET = true
	OVERRIDE_CONFIG_PATH = confFile
	pkg.SetupEnvironmentE2E()
	code := m.Run()
	os.Exit(code)
}
