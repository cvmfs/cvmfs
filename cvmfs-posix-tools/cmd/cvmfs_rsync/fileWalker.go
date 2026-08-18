package main

import (
	"io/fs"
	"path/filepath"
	"sync"
	"time"

	pathlib "github.com/chigopher/pathlib"
	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/trace"
)

const (
	File          string = "file"
	DotSchemeFile string = "dotSchemeFile"
	Directory     string = "directory"
	Symlink       string = "symlink"
	NonExistent   string = "non-existent"
)

type FileSystemObjectInfo interface {
	objectType() string
}

type oGMCarrier struct {
	owner int
	group int
	mode  int
}

type DeleteHolder struct {
	pathString string
	isDir      int
	isFile     int
	isSym      int
}

type FileInfo struct {
	info fs.FileInfo
	oGM  oGMCarrier
}

type DotSchemeFileInfo struct {
	info           fs.FileInfo
	oGM            oGMCarrier
	currentDotFile string
}

type DirInfo struct {
	info        fs.FileInfo
	oGM         oGMCarrier
	aclString   string
	childPathsP *[]SrcDest
}

type SymInfo struct {
	info   fs.FileInfo
	oGM    oGMCarrier
	target string
}

type NonExistentInfo struct {
}

func (fsObj *FileInfo) objectType() string {
	return File
}

func (fsObj *DotSchemeFileInfo) objectType() string {
	return DotSchemeFile
}

func (fsObj *DirInfo) objectType() string {
	return Directory
}

func (fsObj *SymInfo) objectType() string {
	return Symlink
}

func (fsObj *NonExistentInfo) objectType() string {
	return NonExistent
}

type SrcDest struct {
	src  *pathlib.Path
	dest *pathlib.Path
}

// TODO: this may not need to have ctx as a field, should look into that.
type IOObjDoneStatPoolReq struct {
	req         IOInfoStatPoolReq
	childPathsP *[]SrcDest
	ctx         Context
	err         error
}

type SrcDestCtx struct {
	src  *pathlib.Path
	dest *pathlib.Path
	ctx  Context
}

func setupFilewalk(ctx Context, destSrcMap map[*pathlib.Path]*pathlib.Path) ([]SrcDestCtx, map[string]bool) {
	srcDestSlice := []SrcDestCtx{}
	neverDelete := make(map[string]bool)
	for destPath, srcPath := range destSrcMap {
		cleanSrcPath := srcPath.Clean()
		cleanDestPath := destPath.Clean()
		srcDestSlice = append(srcDestSlice, SrcDestCtx{cleanSrcPath, cleanDestPath, ctx})
		neverDelete[cleanDestPath.String()] = true
	}
	return srcDestSlice, neverDelete
}

func fileWalker(ctx Context, repo *pathlib.Path, destSrcMap map[*pathlib.Path]*pathlib.Path, toBeDeleted map[*pathlib.Path]DeletePerms, db pkg.DB) (int, int, error) {
	statReqQueue := make(chan IOInfoStatPoolFastReq, 10000)
	statRespQueue := make(chan IOInfoStatPoolFastResp, 10000)
	linkRewalk := make(chan bool, 10000)
	errChan := make(chan error, 1)

	srcDestSlice, neverDelete := setupFilewalk(ctx, destSrcMap)
	totalLinkRewalks := 0
	totalSrcsCompared := 0
	totalDestsCompared := 0
	totalPurgesTraversed := 0

	var allPurgePaths []*pathlib.Path

	statterWg := sync.WaitGroup{}
	for i := 0; i < ctx.numIOFilewalkWorkers; i++ {
		statterWg.Add(1)
		go func() {
			defer statterWg.Done()
			for req := range statReqQueue {
				processStatInfoReq(req, statRespQueue)
			}
		}()
	}

	processWg := sync.WaitGroup{}
	processWg.Add(1)
	go func() {
		defer processWg.Done()
		linkRewalks := 0
		for range linkRewalk {
			linkRewalks += 1
		}
		totalLinkRewalks = linkRewalks
	}()

	processWg.Add(1)
	go func() {
		defer processWg.Done()
		purgePaths, srcDirents, destDirents, err := processFiles(ctx, neverDelete, toBeDeleted, repo, statRespQueue, db)
		if err != nil {
			errChan <- err
		}
		allPurgePaths = purgePaths
		totalSrcsCompared = srcDirents
		totalDestsCompared = destDirents
	}()

	for _, srcDest := range srcDestSlice {
		ignoredDirs := map[string]bool{}
		srcPath := srcDest.src
		destPath := srcDest.dest
		log.Debug().Msg("Filewalk dir")
		err := filepath.Walk(srcPath.String(), sendToStatterGenerator(ctx, srcPath.String(), destPath, repo, ignoredDirs, statReqQueue, linkRewalk))
		if err != nil {
			log.Error().Err(err).Msg("Error in filewalk/stat")
			return 0, 0, err
		}
	}

	// Need to block here until stat workers are done. Everything else should just work, may not need to do anything else here

	close(linkRewalk)
	close(statReqQueue)
	statterWg.Wait()
	close(statRespQueue)
	processWg.Wait()
	close(errChan)

	// TODO: Optimize this by making it part of the central thread so we can fail out earlier.
	select {
	case err := <-errChan:
		if err != nil {
			log.Error().Err(err).Msg("Errored during filewalk")
			return 0, 0, err
		}
	default:
	}

	if ctx.purge {
		var err error
		totalPurgesTraversed, err = purgeFileWalker(ctx, repo, allPurgePaths, db)
		if err != nil {
			log.Error().Err(err).Msg("Error in purge filewalk")
		}
	}

	return totalLinkRewalks + totalSrcsCompared, totalDestsCompared + totalPurgesTraversed, nil
}

func sendToStatterGenerator(ctx Context, srcStart string, destStart *pathlib.Path, repo *pathlib.Path, ignoredDirs map[string]bool, requests chan IOInfoStatPoolFastReq, linkRewalk chan bool) func(path string, info fs.FileInfo, err error) error {
	return func(src string, srcInfo fs.FileInfo, err error) error {
		log.Debug().Msg("Walk time")
		if err != nil && err.Error() != NotDirectoryError && err != fs.SkipDir {
			log.Debug().Err(err).Msg("Error from filewalk")
			return err
		}
		srcMode := srcInfo.Mode()
		srcPath := pathlib.NewPath(src)
		if ctx.exclude {
			excludeThisPath, err := pkg.EvaluateExclude(srcPath, ctx.excludeStrs...)
			if err != nil {
				log.Error().Err(err).Msg("Error evaluating paths to exclude")
				return err
			}
			if excludeThisPath {
				log.Debug().Str("Path", src).Msg("Excluding following path in filewalk")
				if pathlib.IsDir(srcMode) {
					return fs.SkipDir
				} else {
					return nil
				}
			}
		}
		if srcPath.Name() == pkg.CVMFSProtectedFile || srcPath.Name() == pkg.CVMFSAutoProtectedFile {
			// Silently skip syncing files/dirs/symlinks with protected name
			if pathlib.IsDir(srcMode) {
				return fs.SkipDir
			} else {
				return nil
			}
		}
		rel, err := filepath.Rel(srcStart, src)
		if err != nil {
			// Send error to statter pool here?
			log.Debug().Err(err).Msg("Error with rel path")
			return err
		}
		log.Debug().Str("Rel Path", rel).Msg("Statter Rel Path")
		var destPath *pathlib.Path
		if rel == pkg.CurrentDirectory {
			destPath = destStart
		} else {
			relPath := pathlib.NewPath(rel)
			destPath = destStart.JoinPath(relPath)
		}
		if ctx.linkDeref && pathlib.IsSymlink(srcMode) {
			// I dislike that this is separate from the statter pool logic. It does make more sense here though in the filewalk logic.
			// Maybe a redesign of other versions would have symlinks processed this way instead, it kind of invalidates a "SymDeref" object,
			// instead just being a file/dir rename.
			resWorkingPath, err := srcPath.ResolveAll()
			if err != nil {
				log.Error().Err(err).Str("Path", src).Msg("Error resolving path")
				return err
			}
			resWorkingPathString := resWorkingPath.Clean().String()
			if _, ok := ignoredDirs[resWorkingPathString]; ok {
				return nil
			} else {
				ignoredDirs[resWorkingPathString] = true
			}
			linkRewalk <- true
			err = filepath.Walk(resWorkingPathString, sendToStatterGenerator(ctx, resWorkingPath.String(), destPath, repo, ignoredDirs, requests, linkRewalk))
			if err != nil {
				return err
			}
			ignoredDirs = map[string]bool{}
			return nil
		}
		requests <- IOInfoStatPoolFastReq{srcPath: pathlib.NewPath(src), destPath: destPath, srcInfo: &srcInfo, destInfo: nil, repo: repo, ctx: ctx}
		return nil
	}
}

// Perform the actual filewalk
func doFileWalk(ctx Context, destSrcMap map[*pathlib.Path]*pathlib.Path, repo *pathlib.Path, toBeDeleted map[*pathlib.Path]DeletePerms, db pkg.DB) (int, int, error) {
	var span trace.Span
	ctx.trCtx, span = tr.Start(ctx.trCtx, "doFileWalk")
	defer span.End()
	start_time := time.Now()

	srcDirents, destDirents, err := fileWalker(ctx, repo, destSrcMap, toBeDeleted, db)
	if err != nil {
		log.Error().Err(err).Msg("doFileWalkErr")
		return 0, 0, err
	}

	end_time := time.Now()
	delta := end_time.Sub(start_time).Seconds()
	log.Info().Float64("delta (s)", delta).Msg("Finished File Walk")

	return srcDirents, destDirents, nil

}

// Attempt to push given item onto the given queue and check for livelock. Returns true if successful
func tryPushActual[T any](queue chan T, reqVal T) bool {
	pushed := false
	select {
	case queue <- reqVal:
		pushed = true
	default:
		// log.Debug().Msg("Nothing to pull off queue")
	}
	return pushed
}
