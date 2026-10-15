package pkg

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/go-acl"
	pathlib "github.com/chigopher/pathlib"
	"github.com/pkg/xattr"
	"github.com/rs/zerolog/log"
)

// To be called on paths that are confirmed to exist already, this checks if they can be written to by the user
func UserCanWrite(cfg ConfStruct, pathInfo fs.FileInfo, groupIdMap map[int]bool, uid int, fullDestPath *pathlib.Path) (bool, error) {
	if !cfg.Repo.CurrentGroupConfig.CheckPermissions {
		return true, nil
	}
	owner, group, _, err := GetPathPerms(pathInfo)
	if err != nil {
		return false, err
	}
	canWrite := false
	if pathInfo.Mode()&OtherWriteableMask == OtherWriteableMask {
		canWrite = true
	}
	if _, contains := groupIdMap[group]; contains && pathInfo.Mode()&GroupWriteableMask == GroupWriteableMask {
		canWrite = true
	}
	if owner == uid && pathInfo.Mode()&OwnerWriteableMask == OwnerWriteableMask {
		canWrite = true
	}
	a, err := acl.GetFileAccess(fullDestPath.Clean().String())
	if err != nil {
		panic(err)
	}
	if a != nil {
		defer a.Free()
	}
	for entry := a.FirstEntry(); entry != nil; entry = a.NextEntry() {
		tag, err := entry.GetTag()
		if err != nil {
			return false, err
		}
		// Don't need to check group or user obj, those are already checked
		evalPermForEntry := false
		if tag == acl.TagUser {
			qual, err := entry.GetQualifier()
			if err != nil {
				return false, err
			}
			qualUsr, err := user.LookupId(strconv.Itoa(qual))
			if err != nil {
				log.Error().Err(err).Int("User", qual).Msg("Error getting submitted user")
				return false, err
			}
			qualId, err := strconv.Atoi(qualUsr.Uid)
			if err != nil {
				log.Error().Err(err).Str("Uid", qualUsr.Uid).Msg("Error translating uid")
				return false, err
			}
			log.Debug().Int("User Qualifier", uid).Msg("Qualifier log (USER CAN WRITE)")
			evalPermForEntry = uid == qualId
		} else if tag == acl.TagGroup {
			qual, err := entry.GetQualifier()
			if err != nil {
				return false, err
			}
			qualGrp, err := user.LookupGroupId(strconv.Itoa(qual))
			if err != nil {
				log.Error().Err(err).Int("Group", qual).Msg("Error getting submitted group")
				return false, err
			}
			qualId, err := strconv.Atoi(qualGrp.Gid)
			if err != nil {
				log.Error().Err(err).Str("Gid", qualGrp.Gid).Msg("Error translating gid")
				return false, err
			}
			log.Debug().Int("Group Qualifier", qualId).Msg("Qualifier log (USER CAN WRITE)")
			_, evalPermForEntry = groupIdMap[qualId]
		}

		if evalPermForEntry {
			permset, err := entry.GetPermset()
			if err != nil {
				return false, err
			}
			// This part is very ugly, but the acl api doesn't expose any way to actually check an acl
			if strings.Contains(permset.String(), WriteBitLetter) {
				canWrite = true
			}
		}
	}
	for gid, _ := range groupIdMap {
		log.Debug().Int("Group", gid).Msg("Group id log (USER CAN WRITE)")
	}
	log.Debug().Str("mode", pathInfo.Mode().String()).Str("Acl", a.String()).Int("UID", uid).Msg("Permission info (USER CAN WRITE)")
	return canWrite, nil
}

// Based on the passed in configurations, determines if the user can write to the destpath
func UserCanWriteDir(cfg ConfStruct, destPath, repo *pathlib.Path, destIsDir bool, groupIdMap map[int]bool, uid int) (bool, error) {
	fullDestPath := repo.JoinPath(destPath)
	if !destIsDir {
		fullDestPath = fullDestPath.Parent()
	}
	destInfo, err := os.Stat(fullDestPath.Clean().String())
	if err != nil {
		log.Error().Err(err).Str("Path", fullDestPath.Clean().String()).Msg("Error stating path")
		return false, err
	}
	canWrite, err := UserCanWrite(cfg, destInfo, groupIdMap, uid, fullDestPath)
	if err != nil {
		return false, err
	}
	return canWrite, nil
}

// Get directory contents from the passed in path
func GetDirContentsFromPath(path *pathlib.Path) (dirContents []string, err error) {
	var pathDescriptor *os.File
	pathDescriptor, err = os.Open(path.Clean().String())
	if err != nil {
		log.Error().Err(err).Str("Path", path.Clean().String()).Msg("Failed to open dir to read")
		return dirContents, err
	}
	defer func() {
		if tempErr := pathDescriptor.Close(); tempErr != nil {
			log.Error().Err(tempErr).Msg("Error in cleanup path descriptor")
			if err == nil {
				err = tempErr
			}
		}
	}()
	dirContents, err = pathDescriptor.Readdirnames(0)
	if err != nil {
		log.Error().Err(err).Str("Path", path.Clean().String()).Msg("Failed to read dir")
		return dirContents, err
	}
	return dirContents, err
}

// Determine if a path is empty by only reading one item
func DirEmptyQuick(path *pathlib.Path) (empty bool, err error) {
	var pathDescriptor *os.File
	pathDescriptor, err = os.Open(path.Clean().String())
	if err != nil {
		log.Error().Err(err).Str("Path", path.Clean().String()).Msg("Failed to open dir to read")
		return false, err
	}
	defer func() {
		if tempErr := pathDescriptor.Close(); tempErr != nil {
			log.Error().Err(tempErr).Msg("Error in cleanup path descriptor")
			if err == nil {
				err = tempErr
			}
		}
	}()
	_, err = pathDescriptor.Readdirnames(1)
	if err == io.EOF {
		err = nil
		return true, err
	} else if err != nil {
		log.Error().Err(err).Str("Path", path.Clean().String()).Msg("Failed to read dir")
		return false, err
	}
	return false, err
}

// Read in only 2 items from the passed in path (to be used in determining if cvmfs dir is empty due to catalog)
func getLimitedDirContentsFromPath(path *pathlib.Path) (dirContents []string, err error) {
	var pathDescriptor *os.File
	pathDescriptor, err = os.Open(path.Clean().String())
	if err != nil {
		log.Error().Err(err).Str("Path", path.Clean().String()).Msg("Failed to open dir to read")
		return dirContents, err
	}
	defer func() {
		if tempErr := pathDescriptor.Close(); tempErr != nil {
			log.Error().Err(tempErr).Msg("Error in cleanup path descriptor")
			if err == nil {
				err = tempErr
			}
		}
	}()
	dirContents, err = pathDescriptor.Readdirnames(2)
	if err != nil && err != io.EOF {
		log.Error().Err(err).Str("Path", path.Clean().String()).Msg("Failed to read dir")
		return dirContents, err
	} else if err == io.EOF {
		err = nil
	}
	return dirContents, err
}

// Determines if a cvmfs directory is empty, only reading in 2 items to determine quickly
func CvmfsDirEmptyQuick(fullPath *pathlib.Path) (bool, error) {
	dirContents, err := getLimitedDirContentsFromPath(fullPath)
	if err != nil {
		return false, err
	}
	return len(dirContents) == 1 && (dirContents[0] == CVMFSProtectedFile || dirContents[0] == CVMFSAutoProtectedFile), err
}

// Return if a path exists, and the file info associated with that path with the given statFunc
func pathExistsCommon(path *pathlib.Path, statFunc func(name string) (fs.FileInfo, error)) (bool, fs.FileInfo, error) {
	destExists := true
	fInfo, err := statFunc(path.Clean().String()) // Can do here because it should never have been done before?
	if err != nil {
		if perr, ok := err.(*os.PathError); ok {
			if perr.Timeout() {
				log.Error().Err(err).Str("Str", path.Clean().String()).Msg("Path failed on seeing if it exists - timeout")
				return false, nil, err
			} else if errors.Is(err, fs.ErrPermission) {
				log.Error().Err(err).Str("Str", path.Clean().String()).Msg("Path failed to be read due to permission errors")
				return false, nil, err
			} else {
				log.Debug().Err(err).Msg("Lstat does not exist error, so path does not exist")
				return false, nil, nil
			}
		} else {
			log.Error().Err(err).Str("Str", path.Clean().String()).Msg("Path failed on seeing if it exists")
			return false, nil, err
		}
	}
	return destExists, fInfo, nil
}

// Return if a path exists, and the file info associated with that path
func PathExists(path *pathlib.Path) (bool, fs.FileInfo, error) {
	return pathExistsCommon(path, os.Lstat)
}

// Return if an underlying path exists, and the file info associated with that path
func UnderlyingPathExists(path *pathlib.Path) (bool, fs.FileInfo, error) {
	return pathExistsCommon(path, os.Stat)
}

// returns if this is a path to exclude based on excludeStrPath. Assumes path is a dir
func applicableExclude(path, excludeStrPath *pathlib.Path) (bool, error) {
	excludePathParts := excludeStrPath.Parent().Parts()
	pathParts := path.Parts()
	if len(excludePathParts) == 1 && excludePathParts[0] == CurrentDirectory {
		return true, nil
	}
	if len(excludePathParts) > len(pathParts) {
		return false, nil
	}
	excludeStrParent := strings.Join(excludePathParts, FileDelimeter)
	pathPartsTrimStr := strings.Join(pathParts[len(pathParts)-len(excludePathParts):], FileDelimeter)
	pathMatches, err := filepath.Match(excludeStrParent, pathPartsTrimStr)
	if err != nil {
		log.Error().Err(err).Str("Str", excludeStrParent).Msg("Pattern malformed")
		return false, err
	}
	return pathMatches, nil
}

// Get the contents of a directory without the excluded paths
func getDirContentsWithoutExcluded(cfg ConfStruct, dirContents []*pathlib.Path, path *pathlib.Path, excludeStrPathName string) ([]*pathlib.Path, error) {
	excludeContents, err := path.Glob(excludeStrPathName)
	if err != nil {
		log.Error().Err(err).Str("Path", path.Clean().String()).Msg("Failed to glob inner path")
		return dirContents, err
	}
	// This probably doesn't need to be checked if the path is src instead of dest
	if cfg.Repo.DotScheme {
		excludeContentsDotScheme, err := path.Glob("." + excludeStrPathName + ".*")
		if err != nil {
			log.Error().Err(err).Str("Path", path.Clean().String()).Msg("Failed to glob inner dot path")
			return dirContents, err
		}
		excludeContents = append(excludeContents, excludeContentsDotScheme...)
	}

	if len(excludeContents) > 0 {
		var dirExcludeContents []*pathlib.Path
		excludeMap := make(map[string]struct{})
		for _, path := range excludeContents {
			log.Debug().Str("Path", path.Clean().String()).Msg("Excluding following path in dir read")
			excludeMap[path.Clean().String()] = struct{}{}
		}
		for _, path := range dirContents {
			if _, exclude := excludeMap[path.Clean().String()]; !exclude {
				dirExcludeContents = append(dirExcludeContents, path)
			}
		}
		return dirExcludeContents, nil
	}
	return dirContents, nil
}

// Evaluate if a path should be excluded
func EvaluateExclude(path *pathlib.Path, excludeStrs ...string) (excludeThisPath bool, err error) {
	for _, excludeStr := range excludeStrs {
		excludeStrPath := pathlib.NewPath(excludeStr)
		if applicable, err := applicableExclude(path.Parent(), excludeStrPath); err != nil {
			return false, err
		} else if applicable {
			match, err := filepath.Match(excludeStrPath.Name(), path.Name())
			if err != nil {
				return false, err
			}
			if match {
				return true, nil
			}
		}
	}
	return false, nil
}

// Read a directory with awareness of any exclusion paths
func ReadDirExclude(cfg ConfStruct, path *pathlib.Path, exclude bool, excludeStrs ...string) (paths []*pathlib.Path, err error) {
	dirContentsString, err := GetDirContentsFromPath(path)
	if err != nil {
		return nil, err
	}
	var dirContents []*pathlib.Path
	for _, pathString := range dirContentsString {
		dirContents = append(dirContents, path.Join(pathString))
	}
	if exclude {
		for _, excludeStr := range excludeStrs {
			excludeStrPath := pathlib.NewPath(excludeStr)
			if applicable, err := applicableExclude(path, excludeStrPath); err != nil {
				return dirContents, err
			} else if applicable {
				dirContents, err = getDirContentsWithoutExcluded(cfg, dirContents, path, excludeStrPath.Name())
				if err != nil {
					return dirContents, err
				}
			}
		}
	}
	return dirContents, nil
}

// Return if the path is a file, a sym, and a dir
func GetFileSymDir(pathInfo fs.FileInfo) (bool, bool, bool) {
	pathMode := pathInfo.Mode()
	return pathlib.IsFile(pathMode), pathlib.IsSymlink(pathMode), pathlib.IsDir(pathMode)
}

// Get the relative path from the symlinks location to it's target.
// Currently unused, here for potential use in the future
func GetSymRelPath(path *pathlib.Path) (string, error) {
	// Get absolute link path
	linkDirAbs, err := filepath.Abs(path.Parent().Clean().String())
	if err != nil {
		log.Error().Err(err).Str("Str", path.Parent().Clean().String()).Msg("Failed to get absolute path")
		return "", err
	}

	// Resolve that symlink, then get the absolute path to that target
	target, err := path.ResolveAll()
	if err != nil {
		log.Error().Err(err).Str("Path", path.Clean().String()).Msg("Failed to resolve path sym rel path")
		return "", err
	}
	targetAbs, err := filepath.Abs(target.Clean().String())
	if err != nil {
		log.Error().Err(err).Str("Path", target.Clean().String()).Msg("Failed to get absolute path")
		return "", err
	}

	// Find the relative path to the target from the link
	targetRelPath, err := filepath.Rel(linkDirAbs, targetAbs)
	if err != nil {
		log.Error().Err(err).Str("Path", linkDirAbs).Msg("Failed to get relative path")
		return "", err
	}

	return targetRelPath, nil
}

// Determines first if either the sources or destination are in cvmfs (only one can be true, true if only one is in cvmfs)
// as well as if the srcs are local or on cvmfs (true if local)
func OnlyOneInCvmfs(srcPaths []*pathlib.Path, destPath *pathlib.Path) (bool, bool, error) {
	destInCvmfs := false
	if destPath == nil {
		return false, false, errors.New("No such path")
	}
	path2_abs_inter, err := filepath.EvalSymlinks(destPath.Clean().String())
	if err != nil {
		if os.IsNotExist(err) {
			path2_abs_inter, err = filepath.EvalSymlinks(destPath.Parent().Clean().String())
		}
		if err != nil {
			log.Error().Err(err).Msg("Exited finding one in cvmfs, eval")
			return false, false, err
		}
	}
	path2_abs, err := filepath.Abs(path2_abs_inter)
	if err != nil {
		log.Error().Err(err).Msg("Exited finding one in cvmfs, abs")
		return false, false, err
	}
	if len(path2_abs) >= len(CvmfsLocation) && path2_abs[:len(CvmfsLocation)] == CvmfsLocation {
		destInCvmfs = true
	}

	for _, srcPath := range srcPaths {
		path1_abs_inter, err := filepath.EvalSymlinks(srcPath.Clean().String())
		if err != nil {
			if os.IsNotExist(err) {
				path1_abs_inter, err = filepath.EvalSymlinks(srcPath.Parent().Clean().String())
			}
			if err != nil {
				log.Error().Err(err).Msg("Exited finding one in cvmfs, eval src")
				return false, false, err
			}
		}
		path1_abs, err := filepath.Abs(path1_abs_inter)
		if err != nil {
			log.Error().Err(err).Msg("Exited finding one in cvmfs, abs src")
			return false, false, err
		}
		srcInCvmfs := len(path1_abs) >= len(CvmfsLocation) && path1_abs[:len(CvmfsLocation)] == CvmfsLocation
		if srcInCvmfs && destInCvmfs {
			return false, true, nil
		} else if !srcInCvmfs && !destInCvmfs {
			return false, false, nil
		}
	}

	return true, destInCvmfs, nil
}

// Returns true if the passed in path is in cvmfs
func DestInCvmfs(destPath *pathlib.Path) (bool, error) {
	path2_abs_inter, err := filepath.EvalSymlinks(destPath.Clean().String())
	if err != nil {
		if os.IsNotExist(err) {
			path2_abs_inter, err = filepath.EvalSymlinks(destPath.Parent().Clean().String())
		}
		if err != nil {
			log.Error().Err(err).Msg("Exited finding dest in cvmfs, eval")
			return false, err
		}
	}
	path2_abs, err := filepath.Abs(path2_abs_inter)
	if err != nil {
		log.Error().Err(err).Msg("Exited finding dest in cvmfs, abs")
		return false, err
	}
	if !(len(path2_abs) >= len(CvmfsLocation) && path2_abs[:len(CvmfsLocation)] == CvmfsLocation) {
		return false, nil
	}

	return true, nil
}

// Ensures dest is in cvmfs and returns the absolute path of the passed in path
func DestInCvmfsFromFilePath(filePath *pathlib.Path) (*pathlib.Path, error) {
	absParentPath, err := GetAbsolutePath(filePath.Clean().Parent())
	if err != nil {
		return filePath, err
	}
	var absPath *pathlib.Path
	if filePath.Name() == CurrentDirectory {
		absPath = absParentPath
		absParentPath = absPath.Parent()
	} else if filePath.Name() == PreviousDirectory {
		absPath = absParentPath.Parent()
		absParentPath = absPath.Parent()
	} else {
		absPath = absParentPath.Join(filePath.Name())
	}
	log.Debug().Str("AbsPath", absPath.String()).Str("AbsParentPath", absParentPath.String()).Msg("Evaluating repo for abs paths")

	destIn, err := DestInCvmfs(absParentPath)
	if err != nil {
		return filePath, err
	}
	if !destIn {
		err := fmt.Errorf("every path should be in cvmfs")
		log.Error().Err(err).Msg("At least one path not in cvmfs")
		return filePath, err
	}
	return absPath, nil
}

// Returns path to repo, destination path relative to repo
func GetRepoPath(path *pathlib.Path) (*pathlib.Path, *pathlib.Path, error) {
	//This whole thing is kind of disgusting but I don't want to keep worrying about it right now
	// Will get to later
	// I use two different rels here. May be worth looking at.
	repoPath, err := filepath.Rel(CvmfsLocation, path.Clean().String())
	if err != nil {
		log.Error().Err(err).Msg("Exited getting repo path")
		return nil, nil, err
	}

	repo := pathlib.NewPath(CvmfsLocation).Join(pathlib.NewPath(repoPath).Parts()[0])
	pathRel, err := path.RelativeTo(repo)
	if err != nil {
		log.Error().Err(err).Msg("Exited getting repo path")
		return nil, nil, err
	}
	return repo, pathRel, nil
}

// Gets the absolute path from a passed in path (eval symlinks and make relative to /)
func GetAbsolutePath(path *pathlib.Path) (*pathlib.Path, error) {
	var returnPath *pathlib.Path
	path_abs_inter, err := filepath.EvalSymlinks(path.Clean().String())
	if os.IsNotExist(err) {
		parent_path_abs_inter, err := filepath.EvalSymlinks(path.Parent().Clean().String())
		if err != nil {
			log.Error().Err(err).Msg("Exited finding absolute path")
			return nil, err
		}
		parent_path_abs, err := filepath.Abs(parent_path_abs_inter)
		if err != nil {
			log.Error().Err(err).Msg("Exited finding absolute path")
			return nil, err
		}
		returnPath = pathlib.NewPath(parent_path_abs).Join(path.Name())
		err = nil
	} else if err != nil {
		log.Error().Err(err).Msg("Exited finding absolute path")
		return nil, err
	} else {
		path_abs, err := filepath.Abs(path_abs_inter)
		if err != nil {
			log.Error().Err(err).Msg("Exited finding absolute path")
			return nil, err
		}
		returnPath = pathlib.NewPath(path_abs)
	}
	return returnPath, nil
}

// Return the longest real path and the ghost path that comes with (Ghost path is part of path that does not yet exist)
func LongestRealPath(path *pathlib.Path) (*pathlib.Path, *pathlib.Path, error) {
	longestPath := path.Clean()
	longestExists := false
	var ghostPath *pathlib.Path
	var err error
	for !longestExists {
		if _, err := os.Lstat(longestPath.Clean().String()); os.IsNotExist(err) { // necessary
			longestExists = false
			err = nil
		} else if err != nil {
			log.Error().Err(err).Str("Str", longestPath.Clean().String()).Msg("Path failed on seeing if it exists in longest real path")
			return nil, nil, err
		} else {
			longestExists = true
		}
		if !longestExists {
			if ghostPath != nil {
				ghostPath = pathlib.NewPath(longestPath.Name()).JoinPath(ghostPath)
			} else {
				ghostPath = pathlib.NewPath(longestPath.Name())
			}
			longestPath = longestPath.Parent()
		}
	}
	// Check here for file or dir///
	pathIsFile, err := longestPath.IsFile()
	if os.IsNotExist(err) {
		pathIsFile = false
		err = nil
	} else if err != nil {
		log.Error().Err(err).Msg("Exited finding longest real path")
		return nil, nil, err
	}
	var objName string
	if pathIsFile {
		objName = longestPath.Name()
		longestPath = longestPath.Parent()
	}
	resLongestPath, err := GetAbsolutePath(longestPath)
	if err != nil {
		return nil, nil, err
	}
	if len(resLongestPath.Clean().String()) <= len(CvmfsLocation) {
		log.Error().Str("Path", resLongestPath.Clean().String()).Err(err).Msg("Destination repo is not mounted  on this host")
		return nil, nil, err
	}
	if pathIsFile {
		resLongestPath = resLongestPath.Join(objName)
	}
	return resLongestPath, ghostPath, nil
}

// Adds any necessary uncreated relative paths to the passed in database
func FillInRelativePath(cfg ConfStruct, srcPath, destPath, repo *pathlib.Path, db DB) error {
	_, ghostPath, err := LongestRealPath(repo.JoinPath(destPath).JoinPath(srcPath))
	if err != nil {
		return err
	}
	if ghostPath != nil {
		if len(ghostPath.Clean().Parts()) >= len(srcPath.Clean().Parts()) {
			srcPathBase := pathlib.NewPath(CurrentDirectory)
			if string(srcPath.String()[0]) == FileDelimeter {
				srcPathBase = pathlib.NewPath(FileDelimeter)
			}
			if err := createRelativePathCVMFS(cfg, srcPathBase, destPath, srcPath, db); err != nil {
				return err
			}
		} else {
			srcPathBase := srcPath
			for x := 0; x < len(ghostPath.Clean().Parts()); x++ {
				srcPathBase = srcPathBase.Parent()
			}
			if err := createRelativePathCVMFS(cfg, srcPathBase, destPath.JoinPath(srcPathBase), ghostPath, db); err != nil {
				return err
			}
		}
	}
	return nil
}

// Create a given ghost path in the provided database
func CreateGhostPathCVMFS(cfg ConfStruct, realPath, ghostPath *pathlib.Path, db DB) error {
	owner, group, _, mode := PermsForGroup(cfg)
	return CreateGhostPathCVMFSGivenMode(cfg, realPath, ghostPath, owner, group, mode, db)
}

// Create a given ghost path with the given owner, group, and mode in the provided database
func CreateGhostPathCVMFSGivenMode(cfg ConfStruct, realPath, ghostPath *pathlib.Path, owner, group, mode int, db DB) error {
	if !cfg.Repo.CurrentGroupConfig.AllowUpload {
		err := fmt.Errorf("you are not allowed to create this path in cvmfs, code cannot continue")
		log.Error().Err(err).Str("Path", realPath.JoinPath(ghostPath).Clean().String()).Msg("Configuration issue")
		return err
	}
	dirPath := realPath
	for _, ghostPart := range ghostPath.Parts() {
		if ghostPart != CurrentDirectory {
			dirPath = dirPath.Join(ghostPart)
			log.Info().Str("Name", dirPath.Clean().String()).Msg("Inserting Dir from ghost")
			// ctx.pathPrefix = dirPath.Clean().String()
			if err := db.InsertDir(dirPath.Clean().String(), mode, time.Now().UnixNano(), owner, group, ""); err != nil {
				return err
			}
		}
	}
	return nil
}

// Create a relative path based on the passed in base and ghost paths in the passed in database
func createRelativePathCVMFS(cfg ConfStruct, srcPathBase, destPathBase, ghostPath *pathlib.Path, db DB) error {
	if !cfg.Repo.CurrentGroupConfig.AllowUpload {
		err := fmt.Errorf("you are not allowed to create this path in cvmfs, code cannot continue")
		log.Error().Err(err).Str("Path", destPathBase.JoinPath(ghostPath).Clean().String()).Msg("Configuration issue")
		return err
	}
	dirPath := destPathBase
	srcPath := srcPathBase
	for _, ghostPart := range ghostPath.Parts() {
		if ghostPart != CurrentDirectory && ghostPart != FileDelimeter {
			srcPath = srcPath.Join(ghostPart)
			dirPath = dirPath.Join(ghostPart)
			log.Info().Str("Name", dirPath.Clean().String()).Msg("Inserting Dir from relative fill")
			owner, group, _, mode := PermsForGroup(cfg)
			aclstring := ""
			if err := db.InsertDir(dirPath.Clean().String(), mode, time.Now().UnixNano(), owner, group, aclstring); err != nil {
				return err
			}
		}
	}
	return nil
}

// Returns if a link is a dot scheme path (expects a symlink)
func IsDotSchemeLink(path *pathlib.Path) (bool, error) {
	destResolvedPath, err := path.ResolveAll()
	if os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		log.Error().Err(err).Str("Path", destResolvedPath.Clean().String()).Msg("Failed to resolve path is dot scheme link")
		return false, err
	}

	parentResolvedPath, err := path.Parent().ResolveAll()
	if os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		log.Error().Err(err).Str("Path", destResolvedPath.Clean().String()).Msg("Failed to resolve parent path is dot scheme link")
		return false, err
	}

	if !parentResolvedPath.Clean().Equals(destResolvedPath.Clean().Parent()) {
		return false, nil
	}

	// ResolveAll returns with trailing slash
	destResolvedPath = destResolvedPath.Clean()
	if isFile, err := destResolvedPath.IsFile(); err != nil { // Maybe should be doing this with stat
		log.Error().Err(err).Str("Path", destResolvedPath.Clean().String()).Msg("Failed attempting to do IsFile")
		return false, err
	} else if !isFile {
		return false, nil
	}
	pathNameSlice := strings.Split(destResolvedPath.Name(), DotSchemeDelimeter)

	return len(pathNameSlice) > 2 && strings.Join(pathNameSlice[1:len(pathNameSlice)-1], DotSchemeDelimeter) == path.Name(), nil
}

// Returns if the passed in dest path is or should be a directory (may require src paths to determine for rsync)
func GetDestIsDir(srcPaths []*pathlib.Path, destPathString string, destRelative, destGhostPath, repo *pathlib.Path, relative bool) (bool, error) {
	var err error
	destIsDir := true
	if destGhostPath == nil {
		destIsDir, err = repo.JoinPath(destRelative).IsDir() // Will check if this is a dir or sym to dir which we want
		if os.IsNotExist(err) {
			destIsDir = false
			err = nil
		} else if err != nil {
			log.Error().Err(err).Str("Path", repo.JoinPath(destRelative).Clean().String()).Msg("Failed to do isDir for path")
			return false, err
		}
	} else {
		if len(srcPaths) == 1 && !relative {
			srcInfo, err := os.Lstat(srcPaths[0].Clean().String()) // necessary
			if err != nil {
				log.Error().Err(err).Str("Path", srcPaths[0].Clean().String()).Msg("Unable to lstat path")
				return false, err
			}
			underlyingPathExists, underlyingSrcInfo, err := UnderlyingPathExists(srcPaths[0])
			if err != nil {
				log.Error().Err(err).Str("Path", srcPaths[0].Clean().String()).Msg("Unable to stat path")
				return false, err
			}
			srcMode := srcInfo.Mode()
			srcPathString := srcPaths[0].String()
			destIsDir = pathlib.IsDir(srcMode) || destPathString[len(destPathString)-1:] == FileDelimeter || (underlyingPathExists && pathlib.IsDir(underlyingSrcInfo.Mode()) && srcPathString[len(srcPathString)-1:] == FileDelimeter)
		} else {
			destIsDir = true
		}
	}
	return destIsDir, nil
}

// Returns the acl string for a given path
func GetAclString(pathString string) (string, error) {
	aclString := ""

	_, err1 := xattr.Get(pathString, "system.posix_acl_access")
	_, err2 := xattr.Get(pathString, "system.posix_acl_default")
	if err1 != nil && errors.Is(err1, syscall.ENODATA) && err2 != nil && errors.Is(err2, syscall.ENODATA) {
		log.Debug().Str("Path", pathString).Msg("No ACL on Path")
		return "", nil
	}

	a, err := acl.GetFileAccess(pathString)
	if err != nil {
		if errors.Is(err, syscall.EOPNOTSUPP) {
			log.Debug().Msg("Failure reading ACL - assuming source is nfsv4")
		} else {
			log.Error().Err(err).Str("Path", pathString).Msg("Failed to get FACL for Path")
			return aclString, err
		}
	}
	if a != nil {
		aclString = a.StringWithOptions(acl.TextNumericIDs)
		a.Free()
	}
	return aclString, nil
}

func GetChunkHashesFromXattrs(path *pathlib.Path) ([][]byte, error) {
	chunkHashHexes := [][]byte{}
	chunkList, err := xattr.Get(path.Clean().String(), ChunkListXattr)
	if err != nil {
		log.Error().Err(err).Msg("Error getting paths chunk hashes")
		return nil, err
	}

	chunkListBytesSlice := bytes.Split(chunkList, []byte(LineSeparator))
	for _, chunkListBytes := range chunkListBytesSlice[1 : len(chunkListBytesSlice)-1] {
		chunkHashStringBytes := bytes.Split(chunkListBytes, []byte(CommaSeparator))[0]
		chunkHashHexBytes, err := hex.DecodeString(string(chunkHashStringBytes))
		if err != nil {
			log.Error().Err(err).Str("Chunk Hash", string(chunkHashStringBytes)).Msg("Unable to decode chunk hash")
			return nil, err
		}
		chunkHashHexes = append(chunkHashHexes, chunkHashHexBytes)
	}
	return chunkHashHexes, err
}
