package main

import (
	"io/fs"
	"path/filepath"
	"sync"

	"github.com/chigopher/pathlib"
	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
	"github.com/rs/zerolog/log"
)

func processPurgeFiles(repo *pathlib.Path, statRespQueue chan IOInfoStatPoolFastResp, db pkg.DB) (int, error) {
	totalDestsTraversed := 0
	for respVal := range statRespQueue {
		totalDestsTraversed += 1
		if respVal.destInfo.objectType() == File {
			workingPath, err := respVal.req.destPath.RelativeTo(repo)
			if err != nil {
				log.Error().Err(err).Msg("Error getting delete path relative to repo")
				return 0, err
			}
			db.InsertPurge(workingPath.Clean().String(), pkg.IsAlternateBucketPath(respVal.ctx.cfg, workingPath))
		}
	}
	return totalDestsTraversed, nil
}

func sendToPurgeStatterGenerator(ctx Context, repo *pathlib.Path, responses chan IOInfoStatPoolFastResp) func(path string, info fs.FileInfo, err error) error {
	return func(pathStr string, pathInfo fs.FileInfo, err error) error {
		log.Debug().Msg("Walk time")
		if err != nil && err.Error() != NotDirectoryError && err != fs.SkipDir {
			log.Debug().Err(err).Msg("Error from filewalk")
			return err
		}
		pathMode := pathInfo.Mode()
		path := pathlib.NewPath(pathStr)
		if path.Name() == pkg.CVMFSProtectedFile || path.Name() == pkg.CVMFSAutoProtectedFile {
			// Silently skip syncing files/dirs/symlinks with protected name
			if pathlib.IsDir(pathMode) {
				return fs.SkipDir
			} else {
				return nil
			}
		}
		if pathlib.IsFile(pathMode) {
			newCtx := ctx
			relDestPath, err := path.RelativeTo(repo)
			if err != nil {
				return err
			}
			newCtx.cfg = pkg.PrefixContext(newCtx.cfg, relDestPath)
			req := IOInfoStatPoolFastReq{srcPath: path, destPath: path, srcInfo: nil, destInfo: &pathInfo, repo: repo, ctx: ctx}
			pathObjInfo, err := getKnownInfoStatPoolFast(req, newCtx, false)
			responses <- IOInfoStatPoolFastResp{req: req, ctx: newCtx, srcInfo: nil, destInfo: pathObjInfo, err: err}
		}
		return err
	}
}

func purgeFileWalker(ctx Context, repo *pathlib.Path, purgePaths []*pathlib.Path, db pkg.DB) (int, error) {
	statRespQueue := make(chan IOInfoStatPoolFastResp, 10000)
	errChan := make(chan error, 1)

	totalPathsTraversed := 0

	processWg := sync.WaitGroup{}
	processWg.Add(1)
	go func() {
		var err error
		defer processWg.Done()
		if totalPathsTraversed, err = processPurgeFiles(repo, statRespQueue, db); err != nil {
			errChan <- err
		}
	}()

	for _, purgePath := range purgePaths {
		log.Debug().Msg("Filewalk purge")
		err := filepath.Walk(purgePath.String(), sendToPurgeStatterGenerator(ctx, repo, statRespQueue))
		if err != nil {
			log.Error().Err(err).Msg("Error in filewalk/stat")
			return 0, err
		}
	}

	close(statRespQueue)
	processWg.Wait()
	close(errChan)

	// TODO: Optimize this by making it part of the central thread so we can fail out earlier.
	select {
	case err := <-errChan:
		if err != nil {
			log.Error().Err(err).Msg("Errored during purge filewalk")
			return 0, err
		}
	default:
	}

	return totalPathsTraversed, nil
}
