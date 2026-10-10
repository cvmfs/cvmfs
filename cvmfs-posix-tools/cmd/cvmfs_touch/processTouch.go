package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"syscall"
	"time"

	acl "github.com/cvmfs/cvmfs/cvmfs-posix-tools/go-acl"
	pathlib "github.com/chigopher/pathlib"
	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
	"github.com/rs/zerolog/log"
)

var timeNow = func() time.Time {
	return time.Now()
}

func touchFile(ctx Context, path, repo *pathlib.Path, pathInfo fs.FileInfo, db pkg.DB) error {
	fullPath := repo.JoinPath(path)
	log.Info().Str("Path", fullPath.Clean().String()).Msg("Changing file")
	owner, group, _, err := pkg.GetPathPerms(pathInfo)
	if err != nil {
		return err
	}
	fHashData, err := ctx.hasher.HashFileFromXattrsWithFallback(pathInfo, fullPath, ctx.cvmfsChunkSize, ctx.numHashers, ctx.compressor, ctx.cfg.Repo.DotScheme)
	if err != nil {
		return err
	}
	newMtime := timeNow()
	log.Debug().Str("Path", fullPath.Clean().String()).Str("mtime", newMtime.String()).Msg("path now has mtime")
	err = db.InsertFile(path.Clean().String(), "", int(pathInfo.Mode()), newMtime.UnixNano(), owner, group,
		pathInfo.Size(), strings.Join(pkg.HashesToStrings(fHashData.Hashes), pkg.CommaSeparator), fmt.Sprintf("%040x", fHashData.Checksum), pathInfo, pkg.BoolToInt(ctx.cfg.Repo.ContentAddressable), pkg.IsAlternateBucketPath(ctx.cfg, path))
	if err != nil {
		return err
	}
	return nil
}

func touchSym(ctx Context, path, repo *pathlib.Path, pathInfo fs.FileInfo, db pkg.DB) error {
	fullPath := repo.JoinPath(path)
	fullPathString := fullPath.Clean().String()
	owner, group, _, err := pkg.GetPathPerms(pathInfo)
	if err != nil {
		return err
	}
	log.Info().Str("Path", fullPathString).Msg("Processing sym")
	target, err := os.Readlink(fullPathString)
	if err != nil {
		log.Error().Err(err).Str("Path", fullPathString).Msg("Unable to readlink of path")
		return err
	}
	newMtime := timeNow()
	log.Debug().Str("Path", fullPath.Clean().String()).Str("mtime", newMtime.String()).Msg("path now has mtime")
	db.InsertLink(path.Clean().String(), target, newMtime.UnixNano(), owner, group, pkg.SkipIfFileOrDir)
	return nil
}

func touchDir(ctx Context, path, repo *pathlib.Path, pathInfo fs.FileInfo, db pkg.DB) error {
	owner, group, _, err := pkg.GetPathPerms(pathInfo)
	if err != nil {
		return err
	}
	fullPath := repo.JoinPath(path)
	fullPathString := fullPath.Clean().String()
	log.Info().Str("Path", fullPathString).Msg("Changing dir")
	// Facl code should go in package
	a, err := acl.GetFileAccess(fullPathString)
	if err != nil {
		if errors.Is(err, syscall.EOPNOTSUPP) {
			log.Debug().Msg("Failure reading ACL - assuming source is nfsv4")
		} else {
			log.Error().Err(err).Str("Path", fullPathString).Msg("Failed to get FACL for Path")
			return err
		}
	}
	defer a.Free()
	aclstring := ""
	if a != nil {
		aclstring = a.StringWithOptions(acl.TextNumericIDs)
	}
	newMtime := timeNow()
	log.Debug().Str("Path", fullPathString).Str("mtime", newMtime.String()).Msg("path now has mtime")
	if err := db.InsertDir(path.Clean().String(), int(pathInfo.Mode()), newMtime.UnixNano(), owner, group, aclstring); err != nil {
		return err
	}
	return nil
}

func touchObj(ctx Context, path, repo *pathlib.Path, db pkg.DB) error {
	var err error
	fullPath := repo.JoinPath(path)
	if !ctx.noDeref {
		log.Debug().Str("Path", fullPath.String()).Msg("Dereferencing path")
		fullPath, err = fullPath.ResolveAll()
		if err != nil {
			log.Error().Err(err).Str("path", fullPath.String()).Msg("Error resolving path")
			return err
		}
		path, err = fullPath.RelativeTo(repo)
		if err != nil {
			log.Error().Err(err).Str("path", fullPath.String()).Msg("Error relating path")
			return err
		}
	}
	fullPathString := fullPath.Clean().String()
	pathInfo, err := os.Lstat(fullPathString)
	if err != nil {
		log.Error().Err(err).Str("Path", fullPathString).Msg("Failed in lstating file")
		return err
	}
	ctx.cfg = pkg.GetBasePathPrefix(ctx.cfg, path)
	userCanTouchPath, err := pkg.UserCanWriteDir(ctx.cfg, path.Parent(), repo, true, ctx.groupIdMap, ctx.uid)
	if err != nil {
		log.Error().Err(err).Str("Path", fullPathString).Msg("Error determining perms")
		return err
	}
	if !userCanTouchPath {
		err = fmt.Errorf("not allowed to touch path")
		log.Error().Err(err).Msg("You do not have permissions to touch this path")
		return err
	}
	switch {
	case pathlib.IsDir(pathInfo.Mode()):
		log.Debug().Str("Path", fullPathString).Msg("Path is dir")
		err = touchDir(ctx, path, repo, pathInfo, db)
	case pathlib.IsFile(pathInfo.Mode()):
		log.Debug().Str("Path", fullPathString).Msg("Path is file")
		err = touchFile(ctx, path, repo, pathInfo, db)
	case pathlib.IsSymlink(pathInfo.Mode()):
		log.Debug().Str("Path", fullPathString).Msg("Path is sym")
		err = touchSym(ctx, path, repo, pathInfo, db)
	default:
		err = fmt.Errorf("unknown object type")
		log.Error().Err(err).Str("Path", fullPathString).Msg("Path is an unknown object type.")
	}
	return err
}

func processTouch(ctx Context, paths []*pathlib.Path, repo *pathlib.Path, db pkg.DB) error {
	for _, path := range paths {
		if err := touchObj(ctx, path, repo, db); err != nil {
			return err
		}
	}
	return nil
}
