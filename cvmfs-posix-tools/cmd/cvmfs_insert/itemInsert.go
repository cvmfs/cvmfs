package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	pathlib "github.com/chigopher/pathlib"
	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
	"github.com/rs/zerolog/log"
)

type FileInput struct {
	idx int
	src string
	dst string
}

func (fi FileInput) GetIdx() int {
	return fi.idx
}

type ItemInsert struct {
	srcPath *pathlib.Path
	dstPath *pathlib.Path

	target string

	fileInfo fs.FileInfo
	hashData pkg.FileHashData

	owner     int
	group     int
	mode      int
	aclString string

	canWriteHint bool
}

func resolveRepo(repo string) (string, string, error) {
	var repoPath = pathlib.NewPath(repo)
	absRepoPath, err := pkg.GetAbsolutePath(repoPath.Clean())
	if err != nil {
		log.Error().Err(err).Msg("Can't resolve absolute path for " + repo)
		return "", "", err
	}

	repoPath, _, err = getRepoPath(absRepoPath)
	if err != nil {
		log.Error().Err(err).Msg("Can't resolve absolute path for " + repo)
		return "", "", err
	}

	return repoPath.String(), repoPath.Name(), nil
}

func (item *ItemInsert) Populate(
	ctx *Context,
	repoPathStr string,
	src string,
	dst string) error {
	var err error
	var srcFilePath = pathlib.NewPath(src).Clean()
	var repoPath = pathlib.NewPath(repoPathStr)

	if !ctx.noDeref {
		srcFilePath, err = pkg.GetAbsolutePath(srcFilePath)
		if err != nil {
			return err
		}
	}

	var dstFilePath = pathlib.NewPath(dst)
	if dstFilePath.Name() == pkg.CurrentDirectory || dstFilePath.Name() == pkg.PreviousDirectory || dstFilePath.Name() == pkg.CVMFSProtectedFile || dstFilePath.Name() == pkg.CVMFSAutoProtectedFile {
		err = fmt.Errorf("path not allowed")
		log.Error().Err(err).Msg("Trying to upload to illegal path ., .., or cvmfs catalog")
		return err
	}
	if filepath.IsAbs(dst) {
		// a cvmfs path have been specified in absolute, making sure this is within our repo
		dstFilePath, err = dstFilePath.RelativeTo(repoPath)
		if err != nil {
			return err
		}
	}
	dstFilePath = dstFilePath.Clean()

	var fileCfg = pkg.GetBasePathPrefix(ctx.cfg, dstFilePath)
	if !fileCfg.Repo.CurrentGroupConfig.AllowUpload {
		log.Error().Str(
			"Path", dstFilePath.String()).Msg(
			"You are not allowed to create this cvmfs repository")
		return errors.New("upload not allowed")
	}

	srcFileStat, err := os.Lstat(srcFilePath.String())
	if err != nil {
		return err
	}

	owner, group, mode, err := pkg.GetPermsForUpload(
		fileCfg, srcFileStat, !srcFileStat.IsDir(), ctx.acls)

	if err != nil {
		return err
	}

	item.srcPath = srcFilePath
	item.dstPath = dstFilePath
	item.fileInfo = srcFileStat
	item.owner = owner
	item.group = group
	item.mode = mode
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

	if pathlib.IsSymlink(srcFileStat.Mode()) {
		target, err := os.Readlink(srcFilePath.String())
		if err != nil {
			return err
		}
		item.target = target
	} else if srcFileStat.IsDir() {
		if ctx.acls != pkg.ACLNone {
			aclstring, err := pkg.GetAclString(srcFilePath.String())
			if err != nil {
				return err
			}
			item.aclString = aclstring
		}
	} else {
		fileHashData, err := ctx.hasher.HashFile(
			srcFileStat, srcFilePath, ctx.cvmfsChunkSize)
		if err != nil {
			return err
		}
		item.hashData = fileHashData
	}
	return nil
}

func (item *ItemInsert) IsDir() bool {
	return pathlib.IsDir(item.fileInfo.Mode())
}

func (item *ItemInsert) IsSymlink() bool {
	return pathlib.IsSymlink(item.fileInfo.Mode())
}

func (item *ItemInsert) GetDestPath() *pathlib.Path {
	return item.dstPath
}

func (item *ItemInsert) GetCanWriteHint() bool {
	return item.canWriteHint

}

func (item *ItemInsert) MaybeGhost() bool {
	return item.IsDir()
}

func (item *ItemInsert) InsertGraft(fileCfg pkg.ConfStruct, uid int, noDeref bool, db *pkg.CvmfsDB) error {
	var fileInfo = item.fileInfo

	if noDeref && item.IsSymlink() {
		log.Debug().Str(
			"Name", item.dstPath.String()).Str(
			"Src", item.srcPath.String()).Msg(
			"Inserting Sym with Src")
		return db.InsertLink(
			item.dstPath.String(),
			item.target,
			fileInfo.ModTime().UnixNano(),
			item.owner,
			item.group,
			pkg.SkipIfFileOrDir,
		)
	}

	if item.IsDir() {
		log.Debug().Str(
			"Name", item.dstPath.String()).Str(
			"Src", item.srcPath.String()).Msg(
			"Inserting Dir with Src")
		return db.InsertDir(
			item.dstPath.String(),
			item.mode,
			fileInfo.ModTime().UnixNano(),
			item.owner,
			item.group,
			item.aclString)

	}

	dstFileParentPath := item.dstPath.Parent()
	srcHashData := &item.hashData
	targetPath := item.dstPath

	if fileCfg.Repo.DotScheme {
		dotSchemeName := pkg.DotSchemeDelimeter +
			item.dstPath.Name() + pkg.DotSchemeDelimeter + fmt.Sprintf("%040x", srcHashData.Checksum)
		targetPath = dstFileParentPath.Join(dotSchemeName).Clean()
		log.Debug().Str(
			"Name", item.dstPath.String()).Str(
			"Target", dotSchemeName).Msg(
			"Inserting link with Target")
		if err := db.InsertLink(
			item.dstPath.String(),
			dotSchemeName,
			fileInfo.ModTime().UnixNano(),
			item.owner,
			item.group,
			pkg.SkipIfFileOrDir,
		); err != nil {
			return err
		}
	}

	log.Debug().Str(
		"Name", targetPath.String()).Str(
		"Src", item.srcPath.String()).Msg(
		"Inserting File with Src")

	return db.InsertFile(
		targetPath.String(),
		item.srcPath.String(),
		item.mode,
		fileInfo.ModTime().UnixNano(),
		item.owner,
		item.group,
		fileInfo.Size(),
		strings.Join(pkg.HashesToStrings(srcHashData.Hashes), ","),
		fmt.Sprintf("%040x", srcHashData.Checksum),
		fileInfo,
		pkg.BoolToInt(fileCfg.Repo.ContentAddressable),
		pkg.IsAlternateBucketPath(fileCfg, targetPath),
	)

}
