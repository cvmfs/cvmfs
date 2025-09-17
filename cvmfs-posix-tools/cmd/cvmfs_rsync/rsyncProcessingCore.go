package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	pathlib "github.com/chigopher/pathlib"
	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
	"github.com/pkg/xattr"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/trace"
)

type DeletePerms struct {
	allowDelete bool
	allowPurge  bool
}

// Sertup the contents of the path to be processed in a filewalk
func setupSrcObjectPathContents(workingCtx Context, path, fullWorkingDestPath *pathlib.Path, destSrcMap map[*pathlib.Path]*pathlib.Path, toBeDeleted map[*pathlib.Path]DeletePerms, deleteFromDestPath, destIsDir, destExists bool) error {
	if !workingCtx.recursive && !workingCtx.dirs {
		err := fmt.Errorf("copying dir in non-recursive mode, exiting")
		log.Error().Err(err).Msg("Please specify -r or -d to copy directories.")
		return err
	}
	if !destIsDir {
		err := fmt.Errorf("trying to rsync multiple objects into a single object, exiting")
		log.Error().Err(err).Msg("Please only put one object into single objects.")
		return err
	}
	childPaths, err := pkg.ReadDirExclude(workingCtx.cfg, path, workingCtx.exclude, workingCtx.excludeStrs...)
	if err != nil {
		return err
	}
	for _, childPath := range childPaths {
		childName := childPath.Name()
		if childName != pkg.CVMFSProtectedFile && childName != pkg.CVMFSAutoProtectedFile {
			destSrcMap[fullWorkingDestPath.Join(childName)] = childPath
		}
	}
	// This whole set up is an artifact of relative paths, otherwise we could just delete once if there was one path that was copying in its whole contents
	if workingCtx.delete && destExists && deleteFromDestPath {
		if !workingCtx.cfg.Repo.CurrentGroupConfig.AllowDelete {
			err := fmt.Errorf("configuration issue")
			log.Error().Err(err).Str("path", fullWorkingDestPath.String()).Msg("Deleting from this directory is denied by configuration policy, Allow Delete is not true for this dir. Can't continue")
			return err
		}
		deletePaths, err := pkg.ReadDirExclude(workingCtx.cfg, fullWorkingDestPath, workingCtx.exclude, workingCtx.excludeStrs...)
		if err != nil {
			return err
		}
		for _, path := range deletePaths {
			if path.Name() != pkg.CVMFSProtectedFile && path.Name() != pkg.CVMFSAutoProtectedFile {
				toBeDeleted[path] = DeletePerms{allowDelete: workingCtx.cfg.Repo.CurrentGroupConfig.AllowDelete, allowPurge: workingCtx.cfg.Repo.CurrentGroupConfig.AllowPurge}
			}
		}
	}
	return nil
}

// Setup a path as is to be processed in the filewalk
func setupSrcObjectPath(workingCtx Context, path, fullWorkingDestPath *pathlib.Path, destSrcMap map[*pathlib.Path]*pathlib.Path, destIsDir bool) error {
	pathInfo, err := os.Lstat(path.Clean().String())
	if err != nil {
		log.Error().Err(err).Msg("Could not lstat a source path, exiting")
		return err
	}
	srcIsDir := pathlib.IsDir(pathInfo.Mode())
	if srcIsDir && !workingCtx.recursive && !workingCtx.dirs {
		err := fmt.Errorf("trying to rsync directory in non-recursive context, exiting")
		log.Error().Err(err).Msg("Please specify -r or -d to continue.")
		return err
	}
	if !destIsDir {
		if srcIsDir && !workingCtx.dirs {
			err := fmt.Errorf("destination must be a directory when copying multiple files, exiting")
			log.Error().Err(err).Msg("Please only put one object into single objects.")
			return err
		} else {
			log.Debug().Str("Dest", fullWorkingDestPath.Clean().String()).Msg("Dest is not dir, copying over")
			destSrcMap[fullWorkingDestPath] = path
		}
	} else {
		log.Debug().Str("Dest", fullWorkingDestPath.Clean().String()).Msg("Dest is dir, copying into")
		if !workingCtx.relative {
			fullWorkingDestPath = fullWorkingDestPath.Join(path.Name())
		}
		destSrcMap[fullWorkingDestPath] = path
	}
	return nil
}

// Iterates through src objects and determines how to turn them into objects for the filewalk to process
func setupSrcObjects(ctx Context, srcPaths []*pathlib.Path, destPath, repo *pathlib.Path, destSrcMap map[*pathlib.Path]*pathlib.Path, toBeDeleted map[*pathlib.Path]DeletePerms, destIsDir, destExists bool, db pkg.DB) error {
	if !destIsDir && len(srcPaths) > 1 {
		err := fmt.Errorf("trying to rsync multiple objects into a single object, exiting")
		log.Error().Err(err).Msg("Please only put one object into single objects.")
		return err
	}
	destPathsDeletedFrom := make(map[string]bool)
	for _, path := range srcPaths {
		if path.Name() == pkg.CVMFSProtectedFile || path.Name() == pkg.CVMFSAutoProtectedFile {
			err := fmt.Errorf("protected file mod")
			log.Warn().Str("Protected File", path.String()).Msg("Refusing to modify cvmfs catalog")
			return err
		}
		treatPathAsContents := path.String()[len(path.String())-1:] == pkg.FileDelimeter || path.Name() == pkg.CurrentDirectory || path.Name() == pkg.PreviousDirectory
		path = path.Clean()
		fullWorkingDestPath := repo.JoinPath(destPath)
		workingCtx := ctx
		if workingCtx.exclude {
			for _, excludeStr := range workingCtx.excludeStrs {
				if matchExclude, err := filepath.Match(excludeStr, path.Name()); err != nil {
					log.Error().Err(err).Str("Path", path.String()).Msg("Match error with exclude string")
					return err
				} else if matchExclude {
					log.Debug().Str("Path", path.String()).Msg("Path matched exclude pattern so it wasn't processed")
					return nil
				}
			}
		}

		if workingCtx.relative {
			workingCtx.cfg = pkg.GetBasePathPrefix(workingCtx.cfg, destPath.JoinPath(path))
			if err := pkg.FillInRelativePath(workingCtx.cfg, path.Parent(), destPath, repo, db); err != nil {
				return err
			}
			fullWorkingDestPath = fullWorkingDestPath.JoinPath(path)
		}
		if treatPathAsContents {
			_, ok := destPathsDeletedFrom[fullWorkingDestPath.Clean().String()]
			if err := setupSrcObjectPathContents(workingCtx, path, fullWorkingDestPath, destSrcMap, toBeDeleted, !ok, destIsDir, destExists); err != nil {
				return err
			}
			destPathsDeletedFrom[fullWorkingDestPath.Clean().String()] = true
		} else {
			if err := setupSrcObjectPath(workingCtx, path, fullWorkingDestPath, destSrcMap, destIsDir); err != nil {
				return err
			}
		}
	}

	return nil
}

// Setup the filewalk's src objects and ghost paths, then call the actual filewalk
func filewalkHook(ctx Context, srcPaths []*pathlib.Path, destRelative, destGhostPath, repo *pathlib.Path, destPathString string, db pkg.DB) (fwDelta float64, fwSrcDirents, fwDestDirents int, err error) {
	start_time := time.Now()
	defer func() {
		end_time := time.Now()
		fwDelta = end_time.Sub(start_time).Seconds()
		log.Info().Float64("delta (s)", fwDelta).Msg("Finished File Walk")
	}()
	destIsDir, err := pkg.GetDestIsDir(srcPaths, destPathString, destRelative, destGhostPath, repo, ctx.relative)
	if err != nil {
		return
	}
	// Create necessary ghost paths and determine if the destination is a directory
	ctx.cfg = pkg.GetBasePathPrefix(ctx.cfg, destRelative)
	canWrite, err := pkg.UserCanWriteDir(ctx.cfg, destRelative, repo, destIsDir || destGhostPath != nil, ctx.groupIdMap, ctx.uid)
	if err != nil {
		return
	}
	if !canWrite {
		err = fmt.Errorf("you do not have write permissions to path, quitting")
		log.Error().Err(err).Str("Path", destRelative.Clean().String()).Msg("Permission error")
		return
	}

	if destGhostPath != nil {
		ctx.cfg = pkg.GetBasePathPrefix(ctx.cfg, destRelative.JoinPath(destGhostPath))
		if destIsDir {
			err = pkg.CreateGhostPathCVMFS(ctx.cfg, destRelative, destGhostPath, db)
		} else {
			err = pkg.CreateGhostPathCVMFS(ctx.cfg, destRelative, destGhostPath.Parent(), db)
		}
		if err != nil {
			return
		}
		destRelative = destRelative.JoinPath(destGhostPath)
	}

	destSrcMap := make(map[*pathlib.Path]*pathlib.Path)
	toBeDeleted := make(map[*pathlib.Path]DeletePerms)
	if err = setupSrcObjects(ctx, srcPaths, destRelative, repo, destSrcMap, toBeDeleted, destIsDir, destGhostPath == nil, db); err != nil {
		return
	}

	fwSrcDirents, fwDestDirents, err = doFileWalk(ctx, destSrcMap, repo, toBeDeleted, db)
	return
}

func getAutotunedUploadCtx(ctx Context, avgFileSize int) Context {
	newCtx := ctx
	if pkg.SmallFileSizeHeuristic < avgFileSize && newCtx.numWorkers > 1 {
		newCtx.numConcurrentUploaders = max(newCtx.numConcurrentUploaders*2, 1)
		newCtx.numUploadHashers = max(newCtx.numUploadHashers*2, 1)
		newCtx.numWorkers = max(newCtx.numWorkers/2, 1)
		if pkg.LargeFileSizeHeuristic < avgFileSize && newCtx.numWorkers > 1 {
			newCtx.numConcurrentUploaders = max(newCtx.numConcurrentUploaders*2, 1)
			newCtx.numUploadHashers = max(newCtx.numUploadHashers*2, 1)
			newCtx.numWorkers = max(newCtx.numWorkers/2, 1)
		}
		log.Debug().Int("num workers", newCtx.numWorkers).Int("uploaders per worker", newCtx.numConcurrentUploaders).Int("hashers per worker", newCtx.numUploadHashers).Msg("Re-Autotuned upload due to large file size.")
	}
	return newCtx
}

// Setup variable for easy testing with graft function
var graft = pkg.Graft

// Perform the rsync filewalk, upload, and graft
func rsync(ctx Context, srcPaths []*pathlib.Path, destRelative, destGhostPath, repo *pathlib.Path, destPathString string) (err error) {
	var span trace.Span
	var fwSrcDirents, fwDestDirents int
	var fwDelta float64
	var graftMetrics pkg.GraftMetrics
	revisionNum := pkg.MissingMetric
	revisionNumBytes, err := xattr.Get(repo.Clean().String(), pkg.MountRevisionXattr)
	if err != nil {
		log.Error().Err(err).Msg("Error getting revision")
	} else {
		revisionNum = string(revisionNumBytes)
	}
	uploadStatistics := UploadStatistics{}
	ctx.trCtx, span = tr.Start(ctx.trCtx, "rsync")
	defer span.End()
	// Setting up DB
	var db *pkg.CvmfsDB
	db, err = pkg.NewCvmfsGraftingDB()
	if err != nil {
		return err
	}
	defer func() {
		if tempErr := db.Teardown(err == nil); tempErr != nil {
			log.Error().Err(tempErr).Msg("Error in cleanup")
			if err == nil {
				err = tempErr
			}
		}
	}()

	// Sets up and performs filewalk
	if fwDelta, fwSrcDirents, fwDestDirents, err = filewalkHook(ctx, srcPaths, destRelative, destGhostPath, repo, destPathString, db); err != nil {
		log.Error().Err(err).Msg("rsyncErr")
		return err
	}

	if fileAvgSize, err := db.QueryFilesAvgSize(); err != nil {
		log.Debug().Err(err).Msg("Unable to get file sizes, skipping optimization")
	} else {
		// Casting data loss is irrelevant to functionality here
		ctx = getAutotunedUploadCtx(ctx, int(fileAvgSize))
	}

	if !ctx.dryrun {
		ctx.uploadHasher = pkg.NewHasher(ctx.numUploadHashers, pkg.IOBufferSize)
		ctx.uploadCompressor = nil
		if ctx.cfg.Repo.ContentAddressable {
			ctx.uploadCompressor = pkg.NewZlibCompressor(pkg.IOBufferSize)
		}
		// Upload files
		var s3Interface S3Interface
		s3Interface, err = newBasicS3Manager(ctx)
		if err != nil {
			return err
		}
		var alternateS3Interface S3Interface
		alternateS3Interface, err = newAlternateS3Manager(ctx)
		if err != nil {
			return err
		}
		if uploadStatistics, err = uploadFiles(ctx, s3Interface, alternateS3Interface, db); err != nil {
			return err
		}
		start_time := time.Now()
		if nameClashes, err := db.FileNameClashes(); err != nil {
			return err
		} else if len(nameClashes) > 0 {
			err = fmt.Errorf("name clashes found with necessary cvmfs naming conventions")
			log.Error().Err(err).Strs("Clashing Names", nameClashes).Msg("Data loss would result from these name conflicts. Please resolve name conflicts to proceed with rsync")
			return err
		}
		end_time := time.Now()
		delta := end_time.Sub(start_time).Seconds()
		log.Info().Float64("delta (s)", delta).Msg("Name Clash Resolution Done")

		// Graft files if necessary
		var dbEmpty bool
		if dbEmpty, err = db.IsDatabaseEmpty(); err != nil {
			return err
		} else if !dbEmpty {
			if ctx.skipGraft {
				log.Info().Msg("Skipping grafting step due to --skip-graft flag")
				db.BackupDatabase("graft.db")
			} else {
				ctx.trCtx, span = tr.Start(ctx.trCtx, "graft")
				if graftMetrics, err = graft(db, repo.Name(), ctx.priority, ctx.debug); err != nil {
					return err
				}
				revisionNum = graftMetrics.Revision
				span.End()
			}
		} else {
			log.Info().Msg("Nothing changed, skipping Grafting step")
		}
		// Purge files if necessary
		if ctx.purge && ctx.delete {
			if err = purge(ctx, db); err != nil {
				return err
			}
		}
	}

	// Creates changelog and prints dryrun
	if err = pkg.CreateChangelog(ctx.dryrun, ctx.changelog, db, revisionNum); err != nil {
		return err
	}

	pkg.SendTelegrafStatistics(TelegrafStats(ctx, repo.Name(), destPathString, fwDelta, fwSrcDirents, fwDestDirents, uploadStatistics, graftMetrics), ctx.telegrafAddr)

	return nil
}

func TelegrafStats(ctx Context, repoName, destDir string, fwDelta float64, fwSrcDirents, fwDestDirents int, uploadStatistics UploadStatistics, graftMetrics pkg.GraftMetrics) string {
	// Core Allotment will be added in the future. It should be in these statistics.
	if graftMetrics.Priority == "" {
		graftMetrics.Priority = pkg.LowPriority
	}
	statisticsString := fmt.Sprintf("ioFwWorkers=%d,computeFwWorkers=%d,fwHashers=%d,uploadWorkers=%d,uploadHashers=%d,uploadUploaders=%d,coreAllotment=%d,numCpus=%d,fwDelta=%f,srcDirents=%d,destDirents=%d,uploadFileCount=%d,uploadDelta=%f,uploadRate=%f,uploadSize=%d,graftDelta=%f,numGraftFiles=%d,numGraftDirs=%d,numGraftLinks=%d,numGraftDeletions=%d",
		ctx.numIOFilewalkWorkers, ctx.numComputeFilewalkWorkers, ctx.numFilewalkHashers, ctx.numWorkers, ctx.numUploadHashers, ctx.numConcurrentUploaders, ctx.coreAllotment, ctx.numCpus, fwDelta, fwSrcDirents, fwDestDirents, uploadStatistics.numFiles, uploadStatistics.delta, uploadStatistics.rate, uploadStatistics.totalSize, graftMetrics.Delta, graftMetrics.Files, graftMetrics.Dirs, graftMetrics.Links, graftMetrics.Deletions)
	return fmt.Sprintf("cvmfs_rsync,user=%s,repo=%s,dest_path=%s,cvmfsRsyncVersion=%s,graftPriority=%s,lease_path=%s %s\n", USERNAME, repoName, destDir, CVMFS_RSYNC_VERSION, graftMetrics.Priority, graftMetrics.LeasePath, statisticsString)
}
