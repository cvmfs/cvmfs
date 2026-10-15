package main

import (
	"fmt"
	"strings"

	"github.com/chigopher/pathlib"
	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
	"github.com/rs/zerolog/log"
)

func compareObjects(ctx Context, srcInfo, destInfo FileSystemObjectInfo, srcPath, destPath *pathlib.Path) (bool, pkg.FileHashData, []string, bool, []*pathlib.Path, []string, error) {
	var err error
	var proceed, deleteDest bool
	var srcHashData pkg.FileHashData
	deletePaths := []*pathlib.Path{}
	neverDeletePaths := []string{destPath.Clean().String()}
	switch srcInfo := srcInfo.(type) {
	case *FileInfo:
		proceed, deleteDest, srcHashData, err = fileProceed(ctx, srcInfo, destInfo, srcPath, destPath)
	case *DirInfo:
		proceed, deleteDest, deletePaths, err = dirProceed(ctx, srcInfo, destInfo)
	case *SymInfo:
		proceed, deleteDest, err = symProceed(ctx, srcInfo, destInfo)
	case *DotSchemeFileInfo:
		proceed, deleteDest, err = dSFileProceed(ctx, srcInfo, destInfo, srcPath, destPath)
	case *NonExistentInfo:
		err = fmt.Errorf("src doesn't exist which shouldn't happen")
		log.Error().Err(err).Str("Path", srcPath.Clean().String()).Msg("Unknown src")
	default:
		err = fmt.Errorf("src is unknown type")
		log.Error().Err(err).Str("Path", srcPath.Clean().String()).Msg("Unknown src")
	}
	if err != nil {
		log.Error().Err(err).Msg("Error comparing file objects")
		return false, pkg.FileHashData{}, nil, false, []*pathlib.Path{}, []string{}, err
	}
	if proceed {
		if !ctx.cfg.Repo.CurrentGroupConfig.AllowUpload {
			err := fmt.Errorf("configuration issue")
			log.Error().Err(err).Str("Path", destPath.Clean().String()).Msg("You are not allowed to create this path in cvmfs, Rsync cannot continue due to allow upload config")
			return false, pkg.FileHashData{}, nil, false, []*pathlib.Path{}, []string{}, err
		}
		log.Debug().Str("Path", destPath.String()).Msg("DESTINATION PATH")
		log.Debug().Str("Src", srcPath.Clean().String()).Msg("Copying src over dest")
	} else {
		log.Debug().Str("Src", srcPath.Clean().String()).Msg("Not copying src over dest")
		dotDestInfo, ok := destInfo.(*DotSchemeFileInfo)
		if ok {
			neverDeletePaths = append(neverDeletePaths, destPath.Parent().Join(dotDestInfo.currentDotFile).Clean().String())
		}
	}
	return proceed, srcHashData, pkg.HashesToStrings(srcHashData.Hashes), deleteDest, deletePaths, neverDeletePaths, err
}

func insertObject(ctx Context, srcInfo FileSystemObjectInfo, srcPath, destPath, repo *pathlib.Path, srcHashData pkg.FileHashData, deleteDest bool, db pkg.DB) error {
	var err error
	// Could break these into different response messages, I'm hoping this check isn't bottle necking
	log.Debug().Str("Src Path", srcPath.Clean().String()).Str("Dest Path", destPath.Clean().String()).Msg("Received ComputeCompareResp")

	workingDestPath := destPath
	workingDestPath, err = destPath.RelativeTo(repo)
	if err != nil {
		return err
	}
	switch srcInfo := srcInfo.(type) {
	case *FileInfo:
		targetPath := workingDestPath
		if ctx.cfg.Repo.DotScheme && (ctx.checksum || ctx.dryrun) {
			// Could move this to worker pool, but this short check is probably not bottle necking
			dotSchemeName := pkg.DotSchemeDelimeter + workingDestPath.Name() + pkg.DotSchemeDelimeter + fmt.Sprintf("%040x", srcHashData.Checksum)
			targetPath = workingDestPath.Parent().Join(dotSchemeName)
			log.Debug().Str("Name", workingDestPath.Clean().String()).Str("Target", dotSchemeName).Msg("Inserting link with Target")
			if err := db.InsertLink(workingDestPath.Clean().String(), dotSchemeName, srcInfo.info.ModTime().UnixNano(), srcInfo.oGM.owner, srcInfo.oGM.group, pkg.SkipIfFileOrDir); err != nil {
				return err
			}
		}

		log.Debug().Str("Name", targetPath.Clean().String()).Str("Src", srcPath.Clean().String()).Msg("Inserting File with Src")
		err = db.InsertFile(targetPath.Clean().String(), srcPath.Clean().String(), srcInfo.oGM.mode, srcInfo.info.ModTime().UnixNano(), srcInfo.oGM.owner,
			srcInfo.oGM.group, srcInfo.info.Size(), strings.Join(pkg.HashesToStrings(srcHashData.Hashes), ","), fmt.Sprintf("%040x", srcHashData.Checksum), srcInfo.info, pkg.BoolToInt(ctx.cfg.Repo.ContentAddressable), pkg.IsAlternateBucketPath(ctx.cfg, targetPath))
	case *DirInfo:
		err = db.InsertDir(workingDestPath.Clean().String(), srcInfo.oGM.mode, srcInfo.info.ModTime().UnixNano(), srcInfo.oGM.owner, srcInfo.oGM.group, srcInfo.aclString)
	case *SymInfo:
		err = db.InsertLink(workingDestPath.Clean().String(), srcInfo.target, srcInfo.info.ModTime().UnixNano(), srcInfo.oGM.owner, srcInfo.oGM.group, pkg.SkipIfFileOrDir)
	case *DotSchemeFileInfo:
		err = fmt.Errorf("Unimplemented")
		log.Error().Err(err).Str("Path", srcPath.Clean().String()).Msg("This object is a dot scheme object, it should not be uploaded in this path")
	default:
		err = fmt.Errorf("unrecognized object type")
		respType := fmt.Sprintf("%T\n", srcInfo)
		log.Error().Err(err).Str("Type", respType).Msg("This object type is unknown")
	}
	if err != nil {
		log.Debug().Err(err).Msg("Error in inserting")
		return err
	}

	if deleteDest {
		err = db.InsertDelete(workingDestPath.String(), 1, 1, 1)
		if err != nil {
			log.Error().Err(err).Msg("Failed in inserting delete")
			return err
		}
	}
	// Should happend outside of function
	// delete(fw.destSrcMap, destPath)
	return nil
}

func processCompare(resVal IOInfoStatPoolFastResp, repo *pathlib.Path, db pkg.DB) ([]*pathlib.Path, []string, []*pathlib.Path, error) {
	var err error

	var deletePaths []*pathlib.Path
	var neverDeletePaths []string
	purgePaths := []*pathlib.Path{}
	if resVal.srcInfo.objectType() == Directory && !resVal.ctx.dirs && !resVal.ctx.recursive {
		err := fmt.Errorf("processing dir in non-recursive context")
		log.Error().Err(err).Str("Path", resVal.req.srcPath.Clean().String()).Msg("Please specify -r to process this path")
		return deletePaths, neverDeletePaths, purgePaths, err
	}
	var proceed, deleteObj bool
	var srcHashData pkg.FileHashData
	proceed, srcHashData, _, deleteObj, deletePaths, neverDeletePaths, err = compareObjects(resVal.ctx, resVal.srcInfo, resVal.destInfo, resVal.req.srcPath, resVal.req.destPath)
	if err != nil {
		return deletePaths, neverDeletePaths, purgePaths, err
	}

	if proceed {
		err = insertObject(resVal.ctx, resVal.srcInfo, resVal.req.srcPath, resVal.req.destPath, repo, srcHashData, deleteObj, db)
		if err != nil {
			log.Error().Err(err).Msg("Failed in insertion")
			return deletePaths, neverDeletePaths, purgePaths, err
		}
		// TODO: This may not be the best place to put purge code, but it will work for now
		if deleteObj && resVal.ctx.purge {
			if !resVal.ctx.cfg.Repo.CurrentGroupConfig.AllowPurge {
				err := fmt.Errorf("configuration issue")
				log.Error().Err(err).Str("path", resVal.req.destPath.String()).Msg("Purging from this directory is denied by configuration policy, Allow Purge is not true for this dir. Cannot continue")
				return deletePaths, neverDeletePaths, purgePaths, err
			}
			purgePaths = append(purgePaths, resVal.req.destPath)
		}
	}
	return deletePaths, neverDeletePaths, purgePaths, err
}

func processFiles(ctx Context, neverDelete map[string]bool, toBeDeleted map[*pathlib.Path]DeletePerms, repo *pathlib.Path, statRespQueue chan IOInfoStatPoolFastResp, db pkg.DB) ([]*pathlib.Path, int, int, error) {
	totalSrcsCompared := 0
	totalDestsCompared := 0
	// Could definitely move compare to the worker statter pool. Keeping here now because not slow.
	allPurgePaths := []*pathlib.Path{}
	for respVal := range statRespQueue {
		totalSrcsCompared += 1
		totalDestsCompared += 1
		if respVal.err != nil {
			log.Error().Err(respVal.err).Msg("Error in stat pool, compute")
			return allPurgePaths, 0, 0, respVal.err
		}
		deletePaths, neverDeletePaths, purgePaths, err := processCompare(respVal, repo, db)
		if err != nil {
			log.Error().Err(err).Msg("Error in comparing objects")
			return allPurgePaths, 0, 0, err
		}
		for _, deletePath := range deletePaths {
			toBeDeleted[deletePath] = DeletePerms{allowDelete: respVal.ctx.cfg.Repo.CurrentGroupConfig.AllowDelete, allowPurge: respVal.ctx.cfg.Repo.CurrentGroupConfig.AllowPurge}
		}
		for _, neverDeletePath := range neverDeletePaths {
			neverDelete[neverDeletePath] = true
		}
		allPurgePaths = append(allPurgePaths, purgePaths...)
	}

	for deletePath, deletePerms := range toBeDeleted {
		if _, contains := neverDelete[deletePath.Clean().String()]; !contains {
			if !deletePerms.allowDelete {
				err := fmt.Errorf("configuration issue")
				log.Error().Err(err).Str("path", deletePath.String()).Msg("Deleting from this directory is denied by configuration policy, Allow Delete is not true for this dir. Can't continue")
				return allPurgePaths, 0, 0, err
			}
			workingDelPath, err := deletePath.RelativeTo(repo)
			if err != nil {
				log.Error().Err(err).Msg("Error getting delete path relative to repo")
			}
			workingDelPathStr := workingDelPath.Clean().String()
			if err := db.InsertDelete(workingDelPathStr, 1, 1, 1); err != nil {
				log.Error().Err(err).Msg("Error inserting deletes")
				return allPurgePaths, 0, 0, err
			}
			if ctx.purge {
				// Need to determine which are files vs not
				// These are necessarily unstatted, so it's actually probably okay to go do a file walk over all of them with stats.
				if !deletePerms.allowPurge {
					err := fmt.Errorf("configuration issue")
					log.Error().Err(err).Str("path", deletePath.String()).Msg("Purging from this directory is denied by configuration policy, Allow Purge is not true for this dir. Can't continue")
					return allPurgePaths, 0, 0, err
				}
				// Maybe don't insert into a db, and instead add to a list of strings.
				allPurgePaths = append(allPurgePaths, deletePath)
			}
		}
	}

	return allPurgePaths, totalSrcsCompared, totalDestsCompared, nil
}
