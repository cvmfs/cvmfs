package main

import (
	"fmt"
	"os"

	pathlib "github.com/chigopher/pathlib"
	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
	"github.com/rs/zerolog/log"
)

func addPurges(ctx Context, path *pathlib.Path, repo *pathlib.Path, db pkg.DB) error {
	childPaths, err := pkg.ReadDirExclude(ctx.cfg, repo.JoinPath(path), false)
	if err != nil {
		return err
	}
	for _, childPath := range childPaths {
		if childPath.Name() != pkg.CVMFSProtectedFile && childPath.Name() != pkg.CVMFSAutoProtectedFile {
			childPathInfo, err := os.Lstat(childPath.Clean().String())
			if err != nil {
				log.Error().Err(err).Str("Path", childPath.Clean().String()).Msg("Failed in lstating path")
				return err
			}
			childPathMode := childPathInfo.Mode()
			switch {
			case pathlib.IsDir(childPathMode):

				newCtx := ctx
				newCtx.cfg = pkg.PrefixContext(ctx.cfg, path.Join(childPath.Name()))
				if err := addPurges(newCtx, path.Join(childPath.Name()), repo, db); err != nil {
					return err
				}
			case pathlib.IsFile(childPathMode):
				log.Debug().Str("Path", path.Join(childPath.Name()).Clean().String()).Msg("Inserting Purge")
				db.InsertPurge(path.Join(childPath.Name()).Clean().String(), pkg.IsAlternateBucketPath(ctx.cfg, path.Join(childPath.Name())))
			default:
				log.Debug().Str("Path", childPath.Clean().String()).Msg("Path isn't a file or dir, skipping in purge traversal")
			}
		}
	}
	return nil
}

func processDelete(ctx Context, deletePath *pathlib.Path, repo *pathlib.Path, db pkg.DB) error {
	pathInfo, err := os.Lstat(repo.JoinPath(deletePath).Clean().String())
	if err != nil {
		log.Error().Err(err).Str("Path", deletePath.Clean().String()).Msg("Could not lstat path")
		return err
	}
	pathMode := pathInfo.Mode()
	ctx.cfg = pkg.GetBasePathPrefix(ctx.cfg, deletePath)
	if !ctx.cfg.Repo.CurrentGroupConfig.AllowDelete {
		err := fmt.Errorf("you are not allowed to delete this path in cvmfs. rm cannot continue")
		log.Error().Err(err).Str("Path", repo.JoinPath(deletePath).Clean().String()).Msg("Configuration issue")
		return err
	}
	canWrite, err := pkg.UserCanWriteDir(ctx.cfg, deletePath, repo, pathlib.IsDir(pathMode), ctx.groupIdMap, ctx.uid)
	if err != nil {
		return err
	}
	if !canWrite {
		err = fmt.Errorf("you do not have write permissions to path, quitting")
		log.Error().Err(err).Str("Path", repo.JoinPath(deletePath).Clean().String()).Msg("Permission error")
		return err
	}
	switch {
	case pathlib.IsFile(pathMode):
		log.Debug().Str("Path", deletePath.Clean().String()).Msg("Removing File")
		if err := db.InsertDelete(deletePath.Clean().String(), 0, 1, 0); err != nil {
			return err
		}
		if ctx.purge {
			if !ctx.cfg.Repo.CurrentGroupConfig.AllowPurge {
				err := fmt.Errorf("you are not allowed to purge this path in cvmfs. rm cannot continue")
				log.Error().Err(err).Str("Path", repo.JoinPath(deletePath).Clean().String()).Msg("Configuration issue")
				return err
			}
			log.Debug().Str("Path", deletePath.Clean().String()).Msg("Inserting Purge")
			db.InsertPurge(deletePath.Clean().String(), pkg.IsAlternateBucketPath(ctx.cfg, deletePath))
		}
	case pathlib.IsSymlink(pathMode):
		log.Debug().Str("Path", deletePath.Clean().String()).Msg("Removing Sym")
		if err := db.InsertDelete(deletePath.Clean().String(), 0, 0, 1); err != nil {
			return err
		}
	case pathlib.IsDir(pathMode):
		log.Debug().Str("Path", deletePath.Clean().String()).Msg("Processing Dir")
		if !ctx.recursive {
			err = fmt.Errorf("cannot delete dir in non-recursive context")
			log.Error().Err(err).Str("Path", deletePath.Clean().String()).Msg("Please specify -r if you would like to delete directories")
			return err
		}
		log.Debug().Str("Path", deletePath.Clean().String()).Msg("Removing Dir")
		if err := db.InsertDelete(deletePath.Clean().String(), 1, 0, 0); err != nil {
			return err
		}
		if ctx.purge {
			if !ctx.cfg.Repo.CurrentGroupConfig.AllowPurge {
				err := fmt.Errorf("you are not allowed to purge this path in cvmfs. rm cannot continue")
				log.Error().Err(err).Str("Path", repo.JoinPath(deletePath).Clean().String()).Msg("Configuration issue")
				return err
			}
			if err := addPurges(ctx, deletePath, repo, db); err != nil {
				return err
			}
		}
	default:
		err := fmt.Errorf("unknown object type")
		log.Error().Err(err).Str("Path", deletePath.Clean().String()).Msg("Path is an unknown object type.")
		return err
	}
	return nil
}

func processDeletes(ctx Context, deletePaths []*pathlib.Path, repo *pathlib.Path, db pkg.DB) error {
	for _, deletePath := range deletePaths {
		if err := processDelete(ctx, deletePath, repo, db); err != nil {
			return err
		}
	}
	return nil
}
