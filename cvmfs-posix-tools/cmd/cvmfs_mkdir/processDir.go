package main

import (
	"fmt"
	"os"
	"time"

	pathlib "github.com/chigopher/pathlib"
	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
	"github.com/rs/zerolog/log"
	mode "github.com/tonistiigi/dchapes-mode"
)

func processDir(ctx Context, dirRel, dirGhost, repo *pathlib.Path, aclString string, db pkg.DB) error {
	if dirGhost == nil {
		return nil
	}
	locIsDir, err := repo.JoinPath(dirRel).IsDir()
	if err != nil {
		log.Error().Err(err).Str("Path", repo.JoinPath(dirRel).Clean().String()).Msg("Unable to is dir on path")
		return err
	}
	if !locIsDir {
		err = fmt.Errorf("trying to create dir in non-dir location")
		log.Error().Err(err).Str("Path", repo.JoinPath(dirRel).Clean().String()).Msg("Path is not a directory. Exiting")
		return err
	}
	fullPath := dirRel.JoinPath(dirGhost)
	ctx.cfg = pkg.GetBasePathPrefix(ctx.cfg, fullPath)
	if !ctx.cfg.Repo.CurrentGroupConfig.AllowUpload {
		err := fmt.Errorf("you are not allowed to create this path in cvmfs. mkdir cannot continue")
		log.Error().Err(err).Str("Path", repo.JoinPath(dirRel).JoinPath(dirGhost).Clean().String()).Msg("Configuration issue")
		return err
	}
	if aclString != "" && !ctx.cfg.Repo.CurrentGroupConfig.AllowAclFlag {
		err := fmt.Errorf("acl flag not allowed for path")
		log.Error().Err(err).Str("Path", ctx.cfg.PathPrefix).Msg("Cannot use acl flag in path")
		return err
	}
	canWrite, err := pkg.UserCanWriteDir(ctx.cfg, dirRel, repo, true, ctx.groupIdMap, ctx.uid)
	if err != nil {
		return err
	}
	if !canWrite {
		err = fmt.Errorf("you do not have write permissions to path, quitting")
		log.Error().Err(err).Str("Path", repo.JoinPath(dirRel).Clean().String()).Msg("Permission error")
		return err
	}
	owner, group, _, dirModeInt := pkg.PermsForGroup(ctx.cfg)
	if len(dirGhost.Clean().Parts()) > 1 {
		if !ctx.parent {
			err := fmt.Errorf("trying to create directory in non-existant directory")
			log.Error().Err(err).Msg("To create parents, specify -p.")
			return err
		}
		if err := pkg.CreateGhostPathCVMFSGivenMode(ctx.cfg, dirRel, dirGhost.Parent(), owner, group, dirModeInt, db); err != nil {
			return err
		}
	}

	dirMode := os.FileMode(uint32(dirModeInt))
	if ctx.modeSet {
		changeSet, err := mode.Parse(ctx.mode)
		if err != nil {
			log.Error().Err(err).Str("Mode", ctx.mode).Msg("Provided mode was not able to be parsed")
			return err
		}
		dirMode = changeSet.Apply(dirMode)
	}

	if err := db.InsertDir(fullPath.Clean().String(), int(dirMode), time.Now().UnixNano(), owner, group, aclString); err != nil {
		return err
	}

	return nil
}

func processDirs(ctx Context, dirRelGhostPaths []RelGhost, repo *pathlib.Path, aclString string, db pkg.DB) error {
	for _, relGhost := range dirRelGhostPaths {
		if err := processDir(ctx, relGhost.relative, relGhost.ghost, repo, aclString, db); err != nil {
			return err
		}
	}
	return nil
}
