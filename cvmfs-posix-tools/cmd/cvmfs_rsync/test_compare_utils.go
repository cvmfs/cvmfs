package main

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"

	acl "github.com/cvmfs/cvmfs/cvmfs-posix-tools/go-acl"
	"github.com/chigopher/pathlib"
	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
)

// Get info of src and dest
func getSrcDestInfo(srcPath, destPath *pathlib.Path) (os.FileInfo, os.FileInfo, error) {
	srcInfo, err := os.Lstat(srcPath.Clean().String())
	if err != nil {
		panic(err)
	}
	destInfo, err := os.Lstat(destPath.Clean().String())
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
		pathStat, err := os.Lstat(path.Clean().String())
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

// Compare two symlinks (true if same)
func compareLinksTest(ctx Context, srcPath, destPath *pathlib.Path, excluded map[string]bool) (bool, error) {
	if ctx.linkDeref {
		return compareLinksDerefTest(ctx, srcPath, destPath, excluded)
	}
	srcInfo, destInfo, err := getSrcDestInfo(srcPath, destPath)
	if err != nil {
		panic(err)
	}
	srcTarget, err := os.Readlink(srcPath.Clean().String())
	if err != nil {
		panic(err)
	}
	destTarget, err := os.Readlink(destPath.Clean().String())
	if err != nil {
		panic(err)
	}
	if destInfo.ModTime().Unix() != srcInfo.ModTime().Unix() || srcTarget != destTarget {
		fmt.Println("Time or target diff")
		fmt.Println(srcInfo.ModTime().Unix())
		fmt.Println(destInfo.ModTime().Unix())
		fmt.Println(destInfo.ModTime().Unix() != srcInfo.ModTime().Unix())
		fmt.Println(srcPath.String())
		fmt.Println(destPath.String())
		fmt.Println(srcTarget)
		fmt.Println(destTarget)
		fmt.Println(srcTarget != destTarget)
		return false, nil
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
			fmt.Println("owner or group diff acls")
			return false, nil
		}
	} else if ctx.acls == pkg.ACLNone {
		testUser, testGroup, _, _ := pkg.PermsForGroup(ctx.cfg)

		if destOwner != testUser || destGroup != testGroup {
			fmt.Println("owner or group diff")
			return false, nil
		}
	}
	return true, nil
}

// Compare two derefed symlinks (true if same)
func compareLinksDerefTest(ctx Context, srcPath, destPath *pathlib.Path, excluded map[string]bool) (bool, error) {
	resSrcPath, err := srcPath.ResolveAll()
	if err != nil {
		panic(err)
	}
	resIsDir, err := resSrcPath.IsDir()
	if err != nil {
		panic(err)
	}
	if resIsDir {
		return compareDirsTest(ctx, resSrcPath, destPath, excluded)
	}
	resIsFile, err := resSrcPath.IsFile()
	if err != nil {
		panic(err)
	}
	if resIsFile {
		fileNameAdd := destPath.Name()
		return compareFilesWDotCheckTest(ctx, resSrcPath, destPath.Parent(), fileNameAdd) // Can improve this, it deserves its own function
	}
	return false, nil
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

// Ensure that all paths in excluded exist in delete
func ensureExclusionDeletions(excluded map[string]bool) bool {
	for path := range excluded {
		_, err := os.Lstat(path)
		if os.IsNotExist(err) {
			fmt.Println("Not everything in exclusion existed")
			fmt.Println(path)
			return false
		} else if err != nil {
			panic(err)
		}
	}
	return true
}

// Ensure that deletes occurred and those objects are gone
func ensureDelete(ctx Context, srcPath, destPath *pathlib.Path, excluded map[string]bool) bool {
	// This does not make for a general test, but this structure covers the test cases I'm considering
	destPermPath, err := destPath.RelativeToStr(pkg.TestMount())
	if err != nil {
		panic(err)
	}
	if _, contains := ctx.cfg.Repo.GroupConfig[destPermPath.Clean().String()]; contains && !ctx.cfg.Repo.GroupConfig[destPermPath.Clean().String()].AllowDelete {
		return true
	}
	srcPathDescriptor, err := os.Open(srcPath.Clean().String())
	if err != nil {
		panic(err)
	}
	srcPathContents, err := srcPathDescriptor.Readdirnames(0)
	if err != nil {
		panic(err)
	}
	srcPathDescriptor.Close()
	destPathDescriptor, err := os.Open(destPath.Clean().String())
	if err != nil {
		panic(err)
	}
	destPathContents, err := destPathDescriptor.Readdirnames(0)
	if err != nil {
		panic(err)
	}
	destPathDescriptor.Close()
	srcPathContentsMap := make(map[string]bool)
	for _, srcPathString := range srcPathContents {
		srcPathContentsMap[srcPathString] = true
	}
	for _, destPathStringName := range destPathContents {
		if destPathStringName == pkg.CVMFSProtectedFile || destPathStringName == pkg.CVMFSAutoProtectedFile {
			continue
		}
		if _, contains := excluded[destPathStringName]; contains {
			continue
		}
		_, contains := srcPathContentsMap[destPathStringName]
		excludedContains := false
		if ctx.exclude {
			_, excludedContains = excluded[destPath.Join(destPathStringName).Clean().String()]
		}
		if !contains && !excludedContains {
			if ctx.cfg.Repo.DotScheme {
				fmt.Println(destPath.Join(destPathStringName).Clean().String())
				destNameSplit := strings.Split(destPathStringName, pkg.DotSchemeDelimeter)
				destNameNoDot := strings.Join(destNameSplit[1:len(destNameSplit)-1], pkg.DotSchemeDelimeter)
				_, containsDot := srcPathContentsMap[destNameNoDot]
				if containsDot {
					resDotLink, err := destPath.Join(destNameNoDot).ResolveAll()
					if err != nil {
						panic(err)
					}
					if resDotLink.Name() != destPathStringName {
						return false
					}
				} else {
					return false
				}
				if !containsDot {
					return false
				}
			} else {
				return false
			}
		}
	}
	return true
}

// Compare the directory contents of the two paths (true if same)
func compareDirContentsTest(ctx Context, srcPath, destPath *pathlib.Path, excluded map[string]bool) (bool, error) {
	if ctx.delete {
		if !ensureDelete(ctx, srcPath, destPath, excluded) {
			fmt.Println("Some files are not deleted, but should be")
			return false, nil
		}
	}
	srcPathDirContents, err := srcPath.ReadDir()
	if err != nil {
		panic(err)
	}
	for _, path := range srcPathDirContents {
		if ctx.exclude {
			if _, contains := excluded[destPath.Join(path.Name()).Clean().String()]; contains {
				destExists := pathExistsTestVersion(destPath.Join(path.Name()))
				if destExists {
					fmt.Println("The destination exists when it should have been excluded")
					fmt.Println(destPath.Join(path.Name()).Clean().String())
					return false, nil
				}
				continue
			}
		}
		res := true
		pathInfo, err := os.Lstat(path.Clean().String())
		if err != nil {
			panic(err)
		}
		pathMode := pathInfo.Mode()
		switch {
		case pathlib.IsSymlink(pathMode):
			res, err = compareLinksTest(ctx, path, destPath.Join(path.Name()), excluded)
			if err != nil {
				panic(err)
			}
		case pathlib.IsDir(pathMode):
			res, err = compareDirsTest(ctx, path, destPath.Join(path.Name()), excluded)
			if err != nil {
				panic(err)
			}
		case pathlib.IsFile(pathMode):
			fileNameAdd := path.Name()
			res, err = compareFilesWDotCheckTest(ctx, path, destPath, fileNameAdd)
			if err != nil {
				panic(err)
			}
		default:
			panic(fmt.Errorf("how did we get here?"))
		}
		if !res {
			return false, nil
		}
	}
	return true, nil
}

// Compare two directories (true if same)
func compareDirsTest(ctx Context, srcPath, destPath *pathlib.Path, excluded map[string]bool) (bool, error) {
	// This does not make for a general test, but this structure covers the test cases I'm considering
	// destPermPath, err := destPath.RelativeToStr(TestMount)
	// if err != nil {
	// 	panic(err)
	// }
	// destPermPath = destPermPath.Parent()
	// if _, contains := ctx.cfg.Repo.GroupConfig[destPermPath.Clean().String()]; contains && !ctx.cfg.Repo.GroupConfig[destPermPath.Clean().String()].AllowUpload {
	// 	return true, nil
	// }
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

	res := true
	if !ctx.dirs {
		res, err = compareDirContentsTest(ctx, srcPath, destPath, excluded)
		if err != nil {
			panic(err)
		}
	}

	return res, nil
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
func verifyRsync(ctx Context, srcPathStrings []string, destPathString string, copyOver bool, excluded map[string]bool) bool {
	// In this function every file will be verified
	// Not only that, it should also ensure that the mode, owner, group, and acls have all been copied as well as expected.
	// That may be some extra hardware, but it's a test and it makes sense
	// For checking that stuff, can just do it based on config, or if -a, pull it from the source file itself
	if ctx.exclude && ctx.delete {
		if !ensureExclusionDeletions(excluded) {
			return false
		}
	}
	destPath := pathlib.NewPath(destPathString)
	for _, srcPathString := range srcPathStrings {
		path := pathlib.NewPath(srcPathString)
		fmt.Println(srcPathString)
		treatAsPathContents := srcPathString[len(srcPathString)-1:] == pkg.FileDelimeter || path.Name() == pkg.CurrentDirectory || path.Name() == pkg.PreviousDirectory
		path = path.Clean()
		if ctx.relative {
			destPath = pathlib.NewPath(destPathString).Join(path.Parent().Clean().String())
		}
		res := true
		pathInfo, err := os.Lstat(path.Clean().String())
		if err != nil {
			panic(err)
		}
		pathMode := pathInfo.Mode()
		switch {
		case pathlib.IsSymlink(pathMode):
			if treatAsPathContents {
				res, err = compareDirContentsTest(ctx, path, destPath, excluded)
				if err != nil {
					panic(err)
				}
			} else {
				cmpLink := destPath
				if !copyOver {
					symNameAdd := path.Name()
					destPathBase := destPath
					destIsDirOrSymDir, err := destPath.IsDir()
					if os.IsNotExist(err) {
						destIsDirOrSymDir = false
						err = nil
					} else if err != nil {
						panic(err)
					}
					if !destIsDirOrSymDir {
						symNameAdd = destPath.Name()
						destPathBase = destPath.Parent()
					}
					cmpLink = destPathBase.Join(symNameAdd)
				}
				if ctx.exclude {
					if _, contains := excluded[cmpLink.Clean().String()]; contains {
						destExists := pathExistsTestVersion(cmpLink)
						if err != nil {
							panic(err)
						}
						if destExists {
							fmt.Println("The destination exists when it should have been excluded")
							fmt.Println(cmpLink.Clean().String())
							return false
						}
						continue
					}
				}
				res, err = compareLinksTest(ctx, path, cmpLink, excluded)
				if err != nil {
					panic(err)
				}
			}
		case pathlib.IsDir(pathMode):
			if treatAsPathContents {
				res, err = compareDirContentsTest(ctx, path, destPath, excluded)
				if err != nil {
					panic(err)
				}
			} else {
				cmpDir := destPath
				if !copyOver {
					cmpDir = destPath.Join(path.Name())
				}
				if ctx.exclude {
					if _, contains := excluded[cmpDir.Clean().String()]; contains {
						destExists := pathExistsTestVersion(cmpDir)
						if err != nil {
							panic(err)
						}
						if destExists {
							fmt.Println("The destination exists when it should have been excluded")
							fmt.Println(cmpDir.Clean().String())
							return false
						}
						continue
					}
				}
				res, err = compareDirsTest(ctx, path, cmpDir, excluded)
				if err != nil {
					panic(err)
				}
			}
		case pathlib.IsFile(pathMode):
			fileNameAdd := path.Name()
			destPathBase := destPath
			destIsDirOrSymDir, err := destPath.IsDir()
			if err != nil {
				panic(err)
			}
			if !destIsDirOrSymDir {
				fileNameAdd = destPath.Name()
				destPathBase = destPath.Parent()
			}
			if ctx.exclude {
				if _, contains := excluded[destPathBase.Join(fileNameAdd).Clean().String()]; contains {
					destExists := pathExistsTestVersion(destPathBase.Join(fileNameAdd))
					if err != nil {
						panic(err)
					}
					if destExists {
						fmt.Println("The destination exists when it should have been excluded")
						fmt.Println(destPathBase.Join(fileNameAdd).Clean().String())
						return false
					}
					continue
				}
			}
			res, err = compareFilesWDotCheckTest(ctx, path, destPathBase, fileNameAdd)
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
	return true
}

func removeDuplicateStr(strSlice []string) []string {
	allKeys := make(map[string]bool)
	list := []string{}
	for _, item := range strSlice {
		if _, value := allKeys[item]; !value {
			allKeys[item] = true
			list = append(list, item)
		}
	}
	return list
}

func mockPurgeMaker(filesToPurge []string) func(ctx Context, db pkg.DB) error {
	return func(ctx Context, db pkg.DB) error {
		fmt.Println("Checking Purge")
		purgeSlice := db.QueryPurges()
		purgeStrs := []string{}
		// purgeSlice = removeDuplicateStr(purgeSlice)
		for _, purgeObj := range purgeSlice {
			purgeStrs = append(purgeStrs, purgeObj.PathStr)
		}
		sort.Strings(purgeStrs)
		sort.Strings(filesToPurge)
		if len(purgeSlice) != len(filesToPurge) {
			err := fmt.Errorf("purging file lists are not equal, erroring")
			fmt.Println("Expected:")
			fmt.Println(filesToPurge)
			fmt.Println("Actual:")
			fmt.Println(purgeSlice)
			return err
		}
		for i, path := range purgeStrs {
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

func setupExternalPurge(filesToPurge []string) func() {
	purgeHold := purge
	purge = mockPurgeMaker(filesToPurge)
	return func() {
		purge = purgeHold
	}
}

// Setup context for test execution
func setupContext(recursive, dirs, delete, linkDeref, dryrun, relative bool, excludeStr string) Context {
	checksum := false
	for _, arg := range os.Args[1:] {
		if strings.ToLower(arg) == "checksum" {
			checksum = true
		}
	}
	ctx := Context{
		recursive:                 recursive,
		dirs:                      dirs,
		delete:                    delete,
		purge:                     false,
		checksum:                  checksum,
		linkDeref:                 linkDeref,
		acls:                      pkg.ACLNone,
		changelog:                 nil,
		dryrun:                    dryrun,
		exclude:                   excludeStr != "",
		excludeStrs:               []string{excludeStr},
		debug:                     true,
		relative:                  relative,
		numWorkers:                48,
		numUploadHashers:          30,
		numFilewalkHashers:        30,
		numConcurrentUploaders:    10,
		numIOFilewalkWorkers:      10,
		numComputeFilewalkWorkers: 10,
		// autotuneUploadThreads:     false,
		// autotuneFilewalkThreads:   false,
		// channelSize:               50000,
		trCtx:   context.Background(),
		numCpus: 1,
	}

	var err error
	ctx.cfg, _, ctx.uid, ctx.groupIdMap, err = pkg.GetCvmfsConfigurationInfo(
		pkg.TestMountName(),
		pathlib.NewPath(OVERRIDE_CONFIG_PATH),
	)
	if err != nil {
		panic(err)
	}
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
	graft = pkg.Mock_graft_getter()
	newBasicS3InterfaceHold := newBasicS3Manager
	newBasicS3Manager = newMockBasicS3Manager
	onlyOneInCvmfsHold := onlyOneInCvmfs
	onlyOneInCvmfs = func(srcPaths []*pathlib.Path, destPath *pathlib.Path) (bool, bool, error) { return true, true, nil }
	getRepoPathHold := getRepoPath
	getRepoPath = pkg.MockGetRepoPath
	return func() {
		graft = graftHold
		newBasicS3Manager = newBasicS3InterfaceHold
		onlyOneInCvmfs = onlyOneInCvmfsHold
		getRepoPath = getRepoPathHold
	}
}
