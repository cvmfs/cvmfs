package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	pathlib "github.com/chigopher/pathlib"
	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
	"github.com/rs/zerolog/log"
)

type FaclInput struct {
	idx      int
	faclFile string
	dst      string
}

func (fi FaclInput) GetIdx() int {
	return fi.idx
}

type ItemFacl struct {
	dstPath *pathlib.Path

	destInfo fs.FileInfo

	owner     int
	group     int
	mode      int
	aclString string

	canWriteHint bool
}

func (item *ItemFacl) Populate(
	ctx *Context,
	repoPathStr string,
	faclFile string,
	dst string) error {
	var err error
	var repoPath = pathlib.NewPath(repoPathStr)
	var dstFilePath = pathlib.NewPath(dst)
	if filepath.IsAbs(dst) {
		// a cvmfs path have been specified in absolute, making sure this is within our repo
		dstFilePath, err = dstFilePath.RelativeTo(repoPath)
		if err != nil {
			return err
		}
	}
	dstFilePath = dstFilePath.Clean()
	dstFileStat, err := os.Lstat(repoPath.JoinPath(dstFilePath).String())
	if err != nil {
		log.Error().Err(err).Str("Dest", dstFilePath.String()).Msg("Can't stat dest")
		return err
	}

	if !pathlib.IsDir(dstFileStat.Mode()) {
		err = fmt.Errorf("incorrect file type")
		log.Error().Err(err).Str("Dest", dst).Msg("Dest for facl mod is not a dir. facl change only works over dirs.")
		return err
	}
	if dstFilePath.Name() == pkg.CurrentDirectory || dstFilePath.Name() == pkg.PreviousDirectory || dstFilePath.Name() == pkg.CVMFSProtectedFile || dstFilePath.Name() == pkg.CVMFSAutoProtectedFile {
		err = fmt.Errorf("path not allowed")
		log.Error().Err(err).Msg("Trying to upload to illegal path ., .., or cvmfs catalog")
		return err
	}

	var fileCfg = pkg.GetBasePathPrefix(ctx.cfg, dstFilePath)
	if !fileCfg.Repo.CurrentGroupConfig.AllowUpload {
		log.Error().Str(
			"Path", dstFilePath.String()).Msg(
			"You are not allowed to create this cvmfs repository")
		return errors.New("upload not allowed")
	}

	owner, group, mode, err := pkg.GetPathPerms(dstFileStat)
	if err != nil {
		return err
	}

	aclString, err := pkg.GetAclFromFile(faclFile)
	if err != nil {
		return err
	}

	item.dstPath = dstFilePath
	item.destInfo = dstFileStat
	item.owner = owner
	item.group = group
	item.mode = mode
	item.aclString = aclString
	item.canWriteHint = false

	// This is best effort to try to see if we can write to the parent directory in parallel
	// This call might fail if the parent directory doesn't exist yet, this is why it's only a "hint"
	{
		parentPath := repoPath.JoinPath(dstFilePath).Parent()
		parentInfo, err := os.Stat(parentPath.String())
		if err == nil {
			canWrite, err := pkg.UserCanWrite(fileCfg, parentInfo, ctx.groupIdMap, ctx.uid, parentPath)
			if err == nil && canWrite {
				item.canWriteHint = true
			}
		}
	}

	return nil
}

func (item *ItemFacl) GetDestPath() *pathlib.Path {
	return item.dstPath
}

func (item *ItemFacl) GetCanWriteHint() bool {
	return item.canWriteHint
}

func (item *ItemFacl) MaybeGhost() bool {
	return false
}

func verifyOwner(uid int, fileOwner int) bool {
	return uid == fileOwner
}

func (item *ItemFacl) InsertGraft(fileCfg pkg.ConfStruct, uid int, noDeref bool, db *pkg.CvmfsDB) error {
	var destInfo = item.destInfo
	newFileCfg := pkg.GetBasePathPrefix(fileCfg, item.dstPath)
	if newFileCfg.Repo.CurrentGroupConfig.CheckPermissions && !verifyOwner(uid, item.owner) {
		err := fmt.Errorf("permission error")
		log.Error().Err(err).Str("Path", item.dstPath.Clean().String()).Msg("You do not have permissions to mod facl on this path")
		return err
	}
	log.Debug().Str("Name", item.dstPath.String()).Msg("Changing facl of Dir")
	return db.InsertDir(
		item.dstPath.String(),
		item.mode,
		destInfo.ModTime().UnixNano(),
		item.owner,
		item.group,
		item.aclString)
}
