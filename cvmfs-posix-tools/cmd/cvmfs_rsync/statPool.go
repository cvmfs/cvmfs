package main

import (
	"fmt"
	"io/fs"
	"os"

	pathlib "github.com/chigopher/pathlib"
	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
	"github.com/rs/zerolog/log"
)

// TODO: We really probably don't need ctx here? Actually we might for group permissioning stuff, but maybe that should be checked elsewhere anyway.
// TODO: remove repo, can just pass it to worker on initialization
type IOInfoStatPoolReq struct {
	src      bool
	srcPath  *pathlib.Path
	destPath *pathlib.Path
	path     *pathlib.Path
	repo     *pathlib.Path
	ctx      Context
}

type IOInfoStatPoolResp struct {
	req  IOInfoStatPoolReq
	ctx  Context
	info FileSystemObjectInfo
	err  error
}

type IOInfoStatPoolFastReq struct {
	srcPath  *pathlib.Path
	destPath *pathlib.Path
	srcInfo  *fs.FileInfo
	destInfo *fs.FileInfo
	repo     *pathlib.Path
	ctx      Context
}

type IOInfoStatPoolFastResp struct {
	req      IOInfoStatPoolFastReq
	ctx      Context
	srcInfo  FileSystemObjectInfo
	destInfo FileSystemObjectInfo
	err      error
}

// Read dir
func readDir(ctx Context, srcPath, destPath *pathlib.Path, workingPath *pathlib.Path, src bool) ([]SrcDest, error) {

	var srcDirChildren []SrcDest
	childrenNames := make(map[string]bool)
	childPaths, err := pkg.ReadDirExclude(ctx.cfg, workingPath, ctx.exclude, ctx.excludeStrs...)
	if err != nil {
		return nil, err
	}

	for _, childPath := range childPaths {
		childName := childPath.Name()
		if childName != pkg.CVMFSProtectedFile && childName != pkg.CVMFSAutoProtectedFile {
			if src {
				srcDirChildren = append(srcDirChildren, SrcDest{src: childPath, dest: destPath.Join(childName)})
			} else {
				srcDirChildren = append(srcDirChildren, SrcDest{src: srcPath.Join(childName), dest: childPath})
			}
			childrenNames[childName] = true
		}
	}
	return srcDirChildren, nil
}

// Get information for a file and send it as a response
func getFileInfo(pathInfo fs.FileInfo, oGM oGMCarrier) FileSystemObjectInfo {
	return &FileInfo{info: pathInfo, oGM: oGM}
}

// Get information for a symlink and send it as a response
func getSymInfo(getInfoReq IOInfoStatPoolFastReq, workingPath *pathlib.Path, pathInfo fs.FileInfo, oGM oGMCarrier, ctx Context, src bool) (FileSystemObjectInfo, error) {
	// There may be a smarter way to do this pointer stuff, will figure that out later.
	pathString := workingPath.Clean().String()
	target, err := os.Readlink(pathString)
	if err != nil {
		log.Error().Err(err).Str("Path", pathString).Msg("Couldn't read link for path")
		return nil, err
	}
	// targetPath := pathlib.NewPath(target)
	if !src && getInfoReq.ctx.cfg.Repo.DotScheme { // !getInfoReq.src &&
		isDot, err := pkg.IsDotSchemeLink(workingPath)
		if err != nil {
			return nil, err
		}
		if isDot {
			currentDotFileName := pathlib.NewPath(target).Name()
			dotPathInfo, err := os.Stat(pathString)
			if err != nil {
				log.Error().Err(err).Str("Path", pathString).Msg("Error stating path")
				return nil, err
			}
			return &DotSchemeFileInfo{info: dotPathInfo, currentDotFile: currentDotFileName, oGM: oGM}, nil
		}
	}
	// ONLY APPLICABLE FOR LINK DEREF IN SRC, MAY NOT NEED TO DO? DEPENDS ON HOW WALK HANDLES SYM DEREF
	if getInfoReq.ctx.linkDeref && src {
		// derefReq := getInfoReq
		resWorkingPath, err := workingPath.ResolveAll()
		if err != nil {
			log.Error().Err(err).Str("Path", pathString).Msg("Error resolving path")
			return nil, err
		}
		// Can berak into common function
		pathExists, pathInfo, err := pkg.PathExists(resWorkingPath)
		if err != nil {
			log.Error().Err(err).Str("path", resWorkingPath.String()).Msg("Error stating path")
			return nil, err
		}
		if !pathExists {
			return &NonExistentInfo{}, nil
		}
		owner, group, mode, err := pkg.GetPermsForUpload(ctx.cfg, pathInfo, pathlib.IsFile(pathInfo.Mode()), ctx.acls)
		if err != nil {
			log.Error().Err(err).Str("path", resWorkingPath.String()).Msg("Error getting perms")
			return nil, err
		}
		oGM := oGMCarrier{owner: owner, group: group, mode: mode}
		derefInfo, err := getInfo(getInfoReq, resWorkingPath, pathInfo, oGM, ctx, src)
		if err != nil {
			log.Error().Err(err).Str("Path", pathString).Msg("Error getting deref info")
			return nil, err
		}
		return derefInfo, nil
	}
	return &SymInfo{info: pathInfo, target: target, oGM: oGM}, nil
}

func sendDirInfo(getInfoReq IOInfoStatPoolFastReq, workingPath *pathlib.Path, pathInfo fs.FileInfo, oGM oGMCarrier, ctx Context, src bool) (FileSystemObjectInfo, error) {
	childPaths := []SrcDest{}
	childPathsP := &childPaths
	pathString := workingPath.Clean().String()
	aclstring, err := pkg.GetAclString(pathString)
	if getInfoReq.ctx.acls == pkg.ACLNone && aclstring != "" {
		log.Debug().Msg("ACLNone so dropping acl")
		aclstring = ""
	}
	if err != nil {
		log.Error().Err(err).Str("Path", pathString).Msg("Error getting acl for path")
		return nil, err
	}

	// TODO: We COULD not read the directory if this is a destination and we're not deleting.
	// Should we though? It's just more complexity IMO
	childPaths, err = readDir(ctx, getInfoReq.srcPath, getInfoReq.destPath, workingPath, src)
	if err != nil {
		log.Error().Err(err).Msg("Error reading child paths")
		return nil, err
	}
	return &DirInfo{info: pathInfo, aclString: aclstring, oGM: oGM, childPathsP: childPathsP}, nil
}

func getInfo(getInfoReq IOInfoStatPoolFastReq, workingPath *pathlib.Path, pathInfo fs.FileInfo, oGM oGMCarrier, ctx Context, src bool) (objInfo FileSystemObjectInfo, err error) {
	pathMode := pathInfo.Mode()
	switch {
	// May have to watch this, could be syms in disguise (I don't think so as pathMode should be LStat)
	case pathlib.IsDir(pathMode):
		objInfo, err = sendDirInfo(getInfoReq, workingPath, pathInfo, oGM, ctx, src)
	case pathlib.IsSymlink(pathMode):
		objInfo, err = getSymInfo(getInfoReq, workingPath, pathInfo, oGM, ctx, src)
	case pathlib.IsFile(pathMode):
		objInfo = getFileInfo(pathInfo, oGM)
	default:
		err := fmt.Errorf("unknown object type")
		log.Error().Err(err).Str("Path", workingPath.Clean().String()).Msg("Path is an unknown object type.")
	}
	if err != nil {
		log.Error().Err(err).Str("path", workingPath.String()).Msg("Error getting path info")
	}
	return
}

// Get the necessary stat pool FileSystemObject if fs.FileInfo is known
func getKnownInfoStatPoolFast(getInfoReq IOInfoStatPoolFastReq, ctx Context, src bool) (objInfo FileSystemObjectInfo, err error) {
	var path *pathlib.Path
	var pathInfo fs.FileInfo
	if src {
		path = getInfoReq.srcPath
		pathInfo = *getInfoReq.srcInfo
	} else {
		path = getInfoReq.destPath
		pathInfo = *getInfoReq.destInfo
	}

	pathMode := pathInfo.Mode()
	var owner, group, mode int
	owner, group, mode, err = pkg.GetPermsForUpload(ctx.cfg, pathInfo, pathlib.IsFile(pathMode), ctx.acls)
	if err != nil {
		log.Error().Err(err).Str("path", path.String()).Msg("Error getting perms")
		return
	}
	oGM := oGMCarrier{owner: owner, group: group, mode: mode}

	return getInfo(getInfoReq, path, pathInfo, oGM, ctx, src)
}

// Get the necessary FileSystemObject if fs.FileInfo is unknown
func getUnknownInfoStatPoolFast(getInfoReq IOInfoStatPoolFastReq, ctx Context, src bool) (objInfo FileSystemObjectInfo, err error) {
	var path *pathlib.Path
	if src {
		path = getInfoReq.srcPath
	} else {
		path = getInfoReq.destPath
	}

	pathExists, pathInfo, err := pkg.PathExists(path)
	if err != nil {
		log.Error().Err(err).Str("path", path.String()).Msg("Error stating path")
		return
	}
	if !pathExists {
		objInfo = &NonExistentInfo{}
		return
	}
	owner, group, mode, err := pkg.GetPathPerms(pathInfo)
	if err != nil {
		log.Error().Err(err).Str("path", path.String()).Msg("Error getting perms")
		return
	}
	oGM := oGMCarrier{owner: owner, group: group, mode: mode}

	objInfo, err = getInfo(getInfoReq, path, pathInfo, oGM, ctx, src)
	return
}

func processStatInfoReq(getInfoReq IOInfoStatPoolFastReq, responses chan IOInfoStatPoolFastResp) {
	// Will start by just sending info, can move to doing compare here if this is quick.

	newCtx := getInfoReq.ctx
	relDestPath, err := getInfoReq.destPath.RelativeTo(getInfoReq.repo)
	if err != nil {
		responses <- IOInfoStatPoolFastResp{req: getInfoReq, ctx: newCtx, srcInfo: nil, destInfo: nil, err: err}
		return
	}
	newCtx.cfg = pkg.GetBasePathPrefix(newCtx.cfg, relDestPath)
	getSrcInfo := getKnownInfoStatPoolFast
	if getInfoReq.srcInfo == nil {
		getSrcInfo = getUnknownInfoStatPoolFast
	}
	srcInfo, err := getSrcInfo(getInfoReq, newCtx, true)
	if err != nil {
		log.Debug().Err(err).Msg("Error getting src info")
		responses <- IOInfoStatPoolFastResp{req: getInfoReq, ctx: newCtx, srcInfo: nil, destInfo: nil, err: err}
		return
	}
	getDestInfo := getKnownInfoStatPoolFast
	if getInfoReq.destInfo == nil {
		getDestInfo = getUnknownInfoStatPoolFast
	}
	destInfo, err := getDestInfo(getInfoReq, newCtx, false)
	if err != nil {
		log.Debug().Err(err).Msg("Error getting dest info")
		responses <- IOInfoStatPoolFastResp{req: getInfoReq, ctx: newCtx, srcInfo: nil, destInfo: nil, err: err}
		return
	}

	responses <- IOInfoStatPoolFastResp{req: getInfoReq, ctx: newCtx, srcInfo: srcInfo, destInfo: destInfo, err: err}
}
