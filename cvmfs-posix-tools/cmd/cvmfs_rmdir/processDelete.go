package main

import (
	"fmt"
	"os"

	pathlib "github.com/chigopher/pathlib"
	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
	"github.com/rs/zerolog/log"
)

func processDelete(ctx Context, deletePath *pathlib.Path, repo *pathlib.Path, db pkg.DB) error {
	pathInfo, err := os.Lstat(repo.JoinPath(deletePath).Clean().String())
	if err != nil {
		log.Error().Err(err).Str("Path", deletePath.Clean().String()).Msg("Could not lstat path")
		return err
	}
	pathMode := pathInfo.Mode()
	ctx.cfg = pkg.GetBasePathPrefix(ctx.cfg, deletePath)
	if !ctx.cfg.Repo.CurrentGroupConfig.AllowDelete {
		err := fmt.Errorf("configuration issue")
		log.Error().Err(err).Str("Path", repo.JoinPath(deletePath).Clean().String()).Msg("you are not allowed to delete this path in cvmfs, rmdir cannot continue")
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
	case pathlib.IsDir(pathMode):
		log.Debug().Str("Path", deletePath.Clean().String()).Msg("Processing Dir")
		empty, err := pkg.CvmfsDirEmptyQuick(repo.JoinPath(deletePath))
		if err != nil {
			return err
		}
		if !empty {
			err := fmt.Errorf("dir is not empty")
			log.Error().Err(err).Str("Path", deletePath.Clean().String()).Msg("Cannot delete non-empty directory, please empty")
			return err
		} else {
			log.Debug().Str("Path", deletePath.Clean().String()).Msg("Removing Dir")
			if err := db.InsertDelete(deletePath.Clean().String(), 1, 0, 0); err != nil {
				return err
			}

		}
	default:
		err := fmt.Errorf("non-dir object type")
		log.Error().Err(err).Str("Path", deletePath.Clean().String()).Msg("Path is not a directory")
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
