package main

import (
	"fmt"
	"os"
	"time"

	pathlib "github.com/chigopher/pathlib"
	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
	"github.com/rs/zerolog/log"
)

func processSymlink(ctx Context, target, destPath, repo *pathlib.Path, db pkg.DB) error {
	owner, group, _, _ := pkg.PermsForGroup(ctx.cfg)
	destInfo, err := os.Lstat(repo.JoinPath(destPath).Clean().String())
	if err != nil {
		if os.IsNotExist(err) {
			if err := db.InsertLink(destPath.Clean().String(), target.Clean().String(), time.Now().UnixNano(), owner, group, pkg.SkipIfFileOrDir); err != nil {
				return err
			}
		} else {
			return err
		}
	} else {
		switch {
		case pathlib.IsSymlink(destInfo.Mode()):
			if !ctx.force {
				err := fmt.Errorf("will not overwrite")
				log.Error().Err(err).Msg("You are trying to overwrite a symlink. Specify -f to proceed")
				return err
			} else {
				if err := db.InsertLink(destPath.Clean().String(), target.Clean().String(), time.Now().UnixNano(), owner, group, pkg.SkipIfFileOrDir); err != nil {
					return err
				}
			}
		case pathlib.IsFile(destInfo.Mode()):
			if !ctx.force {
				err := fmt.Errorf("will not overwrite")
				log.Error().Err(err).Msg("You are trying to overwrite a file. Specify -f to proceed")
				return err
			} else if ctx.noDest {
				err := fmt.Errorf("will not overwrite")
				log.Error().Err(err).Msg("path and sym to create are the same")
				return err
			} else {
				if !ctx.cfg.Repo.CurrentGroupConfig.AllowDelete {
					err := fmt.Errorf("configuration issue")
					log.Error().Err(err).Str("Path", repo.JoinPath(destPath).Clean().String()).Msg("You are not allowed to overwrite files. ln cannot continue.")
					return err
				}
				if err := db.InsertDelete(destPath.Clean().String(), 0, 1, 0); err != nil {
					return err
				}
				if err := db.InsertLink(destPath.Clean().String(), target.Clean().String(), time.Now().UnixNano(), owner, group, pkg.SkipIfFileOrDir); err != nil {
					return err
				}
			}
		case pathlib.IsDir(destInfo.Mode()):
			err := fmt.Errorf("cannot overwrite")
			log.Error().Err(err).Msg("You are trying to overwrite a directory which is not allowed. Erroring.")
			return err
		default:
			err := fmt.Errorf("unknown object type")
			log.Error().Err(err).Msg("You are trying to copy over an unknown object type")
			return err
		}
	}
	return nil
}

func processSymlinks(ctx Context, srcLinks []*pathlib.Path, destPath, repo *pathlib.Path, destIsDir bool, db pkg.DB) error {
	if !ctx.cfg.Repo.CurrentGroupConfig.AllowUpload {
		err := fmt.Errorf("configuration issue")
		log.Error().Err(err).Str("Path", repo.JoinPath(destPath).Clean().String()).Msg("You are not allowed to create this path in cvmfs. ln cannot continue.")
		return err
	}
	if destIsDir {
		for _, srcLink := range srcLinks {
			if err := processSymlink(ctx, srcLink, destPath.Join(srcLink.Name()), repo, db); err != nil {
				return err
			}
		}
	} else {
		if len(srcLinks) > 1 {
			err := fmt.Errorf("bad destination")
			log.Error().Err(err).Msg("Destination for symlinks is a single object, cannot put multiple symlinks into it.")
			return err
		}
		if err := processSymlink(ctx, srcLinks[0], destPath, repo, db); err != nil {
			return err
		}
	}
	return nil
}
