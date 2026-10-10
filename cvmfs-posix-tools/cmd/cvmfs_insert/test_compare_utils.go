package main

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"syscall"

	acl "github.com/cvmfs/cvmfs/cvmfs-posix-tools/go-acl"
	"github.com/chigopher/pathlib"
	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
	"github.com/rs/zerolog/log"
)

type SrcDest struct {
	srcPathString     string
	relDestPathString string // The dest path specified here and in the tests is relative
}

func genInsertCsv(pathPairs []SrcDest, useFacl bool) string {
	f, err := os.CreateTemp("", "testInsertCsv.csv")
	if err != nil {
		panic(err)
	}
	insertString := ""
	indicatorString := "insert"
	if useFacl {
		indicatorString = "facl"
	}
	for _, pathPair := range pathPairs {
		insertString += indicatorString + "," + pathPair.srcPathString + "," + pathPair.relDestPathString + "\n"
	}
	f.Write([]byte(insertString))
	if err = f.Close(); err != nil {
		panic(err)
	}
	return f.Name()
}

// Get info of src and dest
func getSrcDestInfo(srcPath, destPath *pathlib.Path) (os.FileInfo, os.FileInfo, error) {
	srcInfo, err := os.Stat(srcPath.Clean().String())
	if err != nil {
		panic(err)
	}
	destInfo, err := os.Stat(destPath.Clean().String())
	if err != nil {
		panic(err)
	}
	return srcInfo, destInfo, nil
}

// Check if dot scheme link is the same (true if same)
func checkDotschemeLink(ctx Context, srcPath, destPath *pathlib.Path) (bool, error) {
	exists := pathExistsTestVersion(destPath)
	if !exists {
		fmt.Println("Doesn't exist:")
		fmt.Println(destPath.String())
		return false, nil
	}
	srcInfo, destInfo, err := getSrcDestInfo(srcPath, destPath)
	if err != nil {
		panic(err)
	}

	destOwner, destGroup, _, err := pkg.GetPathPerms(destInfo)
	if err != nil {
		panic(err)
	}
	if ctx.acls == pkg.ACLPreserveAll {
		srcOwner, srcGroup, _, err := pkg.GetPathPerms(srcInfo)
		if err != nil {
			panic(err)
		}
		if destOwner != srcOwner || destGroup != srcGroup {
			return false, nil
		}
	} else if ctx.acls == pkg.ACLNone {
		testUser, testGroup, _, _ := pkg.PermsForGroup(ctx.cfg)

		if destOwner != testUser || destGroup != testGroup {
			fmt.Println("owner diff")
			return false, nil
		}
	}
	return true, nil
}

// Compare files, checking their dot links (true if same)
func compareFilesWDotCheckTest(ctx Context, path, destPath *pathlib.Path, fileNameAdd string) (bool, error) {
	if ctx.cfg.Repo.DotScheme {
		linkRes, err := checkDotschemeLink(ctx, path, destPath.Join(fileNameAdd))
		if err != nil {
			panic(err)
		}
		if !linkRes {
			return false, nil
		}
		hasher := pkg.NewHasher(30, pkg.IOBufferSize)
		pathStat, err := os.Stat(path.Clean().String())
		if err != nil {
			panic(err)
		}
		pathHash, err := hasher.HashFile(pathStat, path, ctx.cvmfsChunkSize)
		if err != nil {
			panic(err)
		}
		fileNameAdd = "." + fileNameAdd + "." + fmt.Sprintf("%040x", pathHash.Checksum)
	}
	return compareFilesTest(ctx, path, destPath.Join(fileNameAdd))
}

// Check if path exists in testing (panics)
func pathExistsTestVersion(path *pathlib.Path) bool {
	_, err := os.Lstat(path.Clean().String())
	if os.IsNotExist(err) {
		return false
	} else if err != nil {
		panic(err)
	}
	return true
}

// Compare two directories (true if same)
func compareDirsTest(ctx Context, srcPath, destPath *pathlib.Path) (bool, error) {
	srcInfo, destInfo, err := getSrcDestInfo(srcPath, destPath)
	if err != nil {
		panic(err)
	}
	srcAcl, err := acl.GetFileAccess(srcPath.Clean().String())
	if err != nil {
		panic(err)
	}
	if srcAcl != nil {
		defer srcAcl.Free()
	}
	destAcl, err := acl.GetFileAccess(destPath.Clean().String())
	if err != nil {
		panic(err)
	}
	if destAcl != nil {
		defer destAcl.Free()
	}
	if !reflect.DeepEqual(&srcAcl, &destAcl) { //|| destInfo.ModTime().Unix() != srcInfo.ModTime().Unix() - We no longer sync modtime
		fmt.Println("acls or modtime not equal")
		fmt.Println(srcPath.Clean().String())
		fmt.Println(destPath.Clean().String())
		fmt.Println(srcInfo.ModTime().Unix())
		fmt.Println(destInfo.ModTime().Unix())
		return false, nil
	}
	destOwner, destGroup, destMode, err := pkg.GetPathPerms(destInfo)
	if err != nil {
		panic(err)
	}
	if ctx.acls == pkg.ACLPreserveAll {
		srcOwner, srcGroup, srcMode, err := pkg.GetPathPerms(srcInfo)
		if err != nil {
			panic(err)
		}
		if destMode != srcMode || destOwner != srcOwner || destGroup != srcGroup {
			fmt.Println("owner not equal acl mode")
			fmt.Println(srcPath.Clean().String())
			fmt.Println(destPath.Clean().String())
			fmt.Println(srcMode)
			fmt.Println(destMode)
			fmt.Println(srcOwner)
			fmt.Println(destOwner)
			fmt.Println(srcGroup)
			fmt.Println(destGroup)
			return false, nil
		}
	} else if ctx.acls == pkg.ACLNone {
		testUser, testGroup, _, testMode := pkg.PermsForGroup(ctx.cfg)
		if (destMode != testMode && uint16(destMode) != uint16(testMode)) || destOwner != testUser || destGroup != testGroup {
			fmt.Println("owner not equal")
			fmt.Println(destPath.String())
			fmt.Println(destMode - 2147483647 - 1)
			fmt.Println(testMode)
			fmt.Println(destOwner)
			fmt.Println(testUser)
			fmt.Println(destGroup)
			fmt.Println(testGroup)
			return false, nil
		}
	}

	return true, nil
}

// Compare two files (true if same)
func compareFilesTest(ctx Context, srcPath, destPath *pathlib.Path) (bool, error) {
	srcInfo, destInfo, err := getSrcDestInfo(srcPath, destPath)
	if err != nil {
		panic(err)
	}
	if destInfo.Size() != srcInfo.Size() || destInfo.ModTime().Unix() != srcInfo.ModTime().Unix() { // check hashes? maybe for dotscheme
		fmt.Println(srcPath.Clean().String())
		fmt.Println(destPath.Clean().String())
		fmt.Println(srcInfo.Size())
		fmt.Println(destInfo.Size())
		fmt.Println(srcInfo.ModTime().Unix())
		fmt.Println(destInfo.ModTime().Unix())
		fmt.Println("Size Diff")
		return false, nil
	}
	destOwner, destGroup, destMode, err := pkg.GetPathPerms(destInfo)
	if err != nil {
		panic(err)
	}
	if ctx.acls == pkg.ACLPreserveAll {
		srcOwner, srcGroup, srcMode, err := pkg.GetPathPerms(srcInfo)
		if err != nil {
			panic(err)
		}
		if destMode != srcMode || destOwner != srcOwner || destGroup != srcGroup {
			return false, nil
		}
	} else if ctx.acls == pkg.ACLNone {
		testUser, testGroup, testMode, _ := pkg.PermsForGroup(ctx.cfg)
		if destMode != testMode || destOwner != testUser || destGroup != testGroup {
			fmt.Println("Mode Diff")
			return false, nil
		}
	}
	return true, nil
}

// verify the rsync was successful (true if success)
func verifyInsert(ctx Context, pathPairs []SrcDest, repo string, useFacl bool) bool {
	// In this function every file will be verified
	// Not only that, it should also ensure that the mode, owner, group, and acls have all been copied as well as expected.
	// That may be some extra hardware, but it's a test and it makes sense
	// For checking that stuff, can just do it based on config, or if -a, pull it from the source file itself
	repoPath := pathlib.NewPath(repo)
	if !useFacl {
		for _, pathPair := range pathPairs {
			var res bool
			srcPath := pathlib.NewPath(pathPair.srcPathString).Clean()
			destPath := repoPath.Join(pathPair.relDestPathString).Clean()
			pathInfo, err := os.Stat(srcPath.Clean().String())
			if err != nil {
				panic(err)
			}
			pathMode := pathInfo.Mode()
			switch {
			case pathlib.IsDir(pathMode):
				res, err = compareDirsTest(ctx, srcPath, destPath)
				if err != nil {
					panic(err)
				}
			case pathlib.IsFile(pathMode):
				fileNameAdd := destPath.Name()
				destPathBase := destPath.Parent()
				res, err = compareFilesWDotCheckTest(ctx, srcPath, destPathBase, fileNameAdd)
				if err != nil {
					panic(err)
				}
			default:
				fmt.Printf("How did we get here?")
			}
			if !res {
				return false
			}
		}
	} else {
		for _, pathPair := range pathPairs {
			expectedAclString, err := pkg.GetAclFromFile(pathPair.srcPathString)
			if err != nil {
				panic(err)
			}
			ea, err := acl.Parse(expectedAclString)
			if err != nil {
				log.Error().Err(err).Str("Path", pathPair.srcPathString).Msg("Failed to parse expected FACL")
				panic(err)
			}
			defer ea.Free()
			cleanExpectedAclString := ea.StringWithOptions(acl.TextNumericIDs)
			a, err := acl.GetFileAccess(repoPath.Join(pathPair.relDestPathString).String())
			if err != nil {
				if errors.Is(err, syscall.EOPNOTSUPP) {
					log.Debug().Msg("Failure reading ACL - assuming source is nfsv4")
				} else {
					log.Error().Err(err).Str("Path", repoPath.Join(pathPair.relDestPathString).String()).Msg("Failed to get FACL for Path")
					panic(err)
				}
			}
			defer a.Free()
			cleanDestAclString := ""
			if a != nil {
				cleanDestAclString = a.StringWithOptions(acl.TextNumericIDs)
			}
			if !reflect.DeepEqual(cleanExpectedAclString, cleanDestAclString) {
				fmt.Println("acls not equal")
				fmt.Println(pathPair.srcPathString)
				fmt.Println(repoPath.Join(pathPair.relDestPathString).String())
				fmt.Println(cleanExpectedAclString)
				fmt.Println(cleanDestAclString)
				return false
			}
		}
	}
	return true
}

// Setup context for test execution
func setupContext(dryrun bool) Context {
	ctx := Context{
		acls:                   pkg.ACLNone,
		dryrun:                 dryrun,
		debug:                  true,
		numWorkers:             48,
		numHashers:             30,
		numConcurrentUploaders: 10,
		coreAllotment:          4,
	}

	var err error
	ctx.cfg, _, ctx.uid, ctx.groupIdMap, err = pkg.GetCvmfsConfigurationInfo(
		pkg.TestMountName(),
		pathlib.NewPath(OVERRIDE_CONFIG_PATH),
	)
	if err != nil {
		panic(err)
	}
	ctx.hasher = pkg.NewHasher(ctx.numHashers, pkg.IOBufferSize)
	if ctx.cfg.Repo.ContentAddressable {
		ctx.cvmfsChunkSize = pkg.CVMFSInternalChunkSize
	} else {
		ctx.cvmfsChunkSize = pkg.CVMFSChunkSize
	}

	return ctx
}

// Setup external functions so that tests don't interact with non-existant external objects. Returns cleanup function
func setupExternalFuncs() func() {
	graftHold := graft
	graft = pkg.Mock_graft_getter_options()
	newBasicS3InterfaceHold := newBasicS3Manager
	newBasicS3Manager = newMockBasicS3Manager
	// newBasicS3CAInterfaceHold := newBasicS3CAManager
	// newBasicS3CAManager = newMockBasicS3Manager
	getRepoPathHold := getRepoPath
	getRepoPath = pkg.MockGetRepoPath
	return func() {
		graft = graftHold
		newBasicS3Manager = newBasicS3InterfaceHold
		// newBasicS3CAManager = newBasicS3CAInterfaceHold
		getRepoPath = getRepoPathHold
	}
}
