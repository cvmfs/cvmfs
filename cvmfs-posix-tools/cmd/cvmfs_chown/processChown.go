package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/user"
	"strconv"
	"strings"
	"syscall"

	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/go-acl"
	pathlib "github.com/chigopher/pathlib"
	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
	"github.com/rs/zerolog/log"
)

func verifyOwner(ctx Context, fileOwner int) bool {
	return ctx.uid == fileOwner
}

func getOwnerGroup(ctx Context, ownerString string) (int, int, bool, error) {
	var err error
	var ownerId int
	var groupId int
	groupSet := false
	if ctx.referenceSet {
		fInfo, err := os.Stat(ctx.reference)
		if err != nil {
			log.Error().Err(err).Str("Path", ctx.reference).Msg("Failed in stating reference file")
			return 0, 0, false, err
		}
		ownerId, _, _, err = pkg.GetPathPerms(fInfo)
		if err != nil {
			return 0, 0, false, err
		}
	} else {
		ownerStringSplit := strings.Split(ownerString, ownerGroupDelimeter)
		if len(ownerStringSplit) > 2 {
			err = fmt.Errorf("malformed request")
			log.Error().Err(err).Str("Path", ctx.reference).Msg("You have too many : in your request")
			return 0, 0, false, err
		}
		if ownerId, err = strconv.Atoi(ownerStringSplit[0]); err != nil {
			userPtr, err := user.Lookup(ownerStringSplit[0])
			if err != nil {
				log.Error().Err(err).Str("Owner", ownerStringSplit[0]).Msg("Error looking up owner")
				return 0, 0, false, err
			}
			if ownerId, err = strconv.Atoi(userPtr.Uid); err != nil {
				log.Error().Err(err).Str("Id", userPtr.Uid).Msg("Error getting owner id")
				return 0, 0, false, err
			}
		}
		if len(ownerStringSplit) == 2 {
			if groupId, err = strconv.Atoi(ownerStringSplit[1]); err != nil {
				grpPtr, err := user.LookupGroup(ownerStringSplit[1])
				if err != nil {
					log.Error().Err(err).Str("Group", ownerStringSplit[1]).Msg("Error looking up group")
					return 0, 0, false, err
				}
				if groupId, err = strconv.Atoi(grpPtr.Gid); err != nil {
					log.Error().Err(err).Str("Id", grpPtr.Gid).Msg("Error getting group id")
					return 0, 0, false, err
				}
			}
			groupSet = true
		}
	}
	return ownerId, groupId, groupSet, nil
}

func changeFileMode(ctx Context, owner, group, mode int, path, repo *pathlib.Path, pathInfo fs.FileInfo, db pkg.DB) error {
	fullPath := repo.JoinPath(path)
	fHashData, err := ctx.hasher.HashFileFromXattrsWithFallback(pathInfo, fullPath, ctx.cvmfsChunkSize, ctx.numHashers, ctx.compressor, ctx.cfg.Repo.DotScheme)
	if err != nil {
		return err
	}
	log.Debug().Str("Path", repo.JoinPath(path).Clean().String()).Int("Owner", owner).Int("Group", group).Msg("path now has owner/group")
	err = db.InsertFile(path.Clean().String(), "", mode, pathInfo.ModTime().UnixNano(), owner, group,
		pathInfo.Size(), strings.Join(pkg.HashesToStrings(fHashData.Hashes), pkg.CommaSeparator), fmt.Sprintf("%040x", fHashData.Checksum), pathInfo, pkg.BoolToInt(ctx.cfg.Repo.ContentAddressable), pkg.IsAlternateBucketPath(ctx.cfg, path))
	if err != nil {
		return err
	}
	return nil
}

func changeSymMode(ctx Context, owner, group, mode int, groupSet bool, path, repo *pathlib.Path, pathInfo fs.FileInfo, db pkg.DB) error {
	fullPath := repo.JoinPath(path)
	fullPathString := fullPath.Clean().String()
	log.Info().Str("Path", fullPathString).Msg("Processing sym")
	linkTarget, err := os.Readlink(fullPathString)
	if err != nil {
		log.Error().Err(err).Str("Path", fullPathString).Msg("Unable to readlink of path")
		return err
	}
	log.Debug().Str("Path", repo.JoinPath(path).Clean().String()).Int("Owner", owner).Int("Group", group).Msg("sym now has owner/group")
	err = db.InsertLink(path.Clean().String(), linkTarget, pathInfo.ModTime().UnixNano(), owner, group, pkg.SkipIfFileOrDir)
	if err != nil {
		return err
	}
	if ctx.cfg.Repo.DotScheme {
		isDotLink, err := pkg.IsDotSchemeLink(repo.JoinPath(path))
		if err != nil {
			return err
		}
		if isDotLink {
			log.Info().Str("Path", fullPathString).Msg("Dot link sym, processing as file")

			filePathInfo, err := os.Stat(fullPathString)
			if err != nil {
				log.Error().Err(err).Str("Path", fullPathString).Msg("Failed in lstating file")
				return err
			}
			fileOwner, fileGroup, fileMode, err := pkg.GetPathPerms(filePathInfo)
			if err != nil {
				return err
			}
			ctx.cfg = pkg.GetBasePathPrefix(ctx.cfg, path)
			if ctx.cfg.Repo.CurrentGroupConfig.CheckPermissions && !verifyOwner(ctx, fileOwner) {
				err := fmt.Errorf("permission error")
				log.Error().Err(err).Str("Path", path.Clean().String()).Msg("You do not have permissions to chmod on this path")
				return err
			}
			if !groupSet {
				group = fileGroup
			}
			if err := changeFileMode(ctx, owner, group, fileMode, path.Parent().Join(linkTarget), repo, filePathInfo, db); err != nil {
				return err
			}
		}
	}
	return nil
}

func changeDirMode(ctx Context, owner, group, mode int, path, repo *pathlib.Path, pathInfo fs.FileInfo, db pkg.DB) error {
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
		equiv, _, err := a.EquivMode()
		if err != nil {
			log.Error().Err(err).Msg("Failed to run acl_equiv_mode")
		}
		if !equiv {
			aclstring = a.StringWithOptions(acl.TextNumericIDs)
		}
	}
	log.Debug().Str("Path", fullPathString).Int("Owner", owner).Int("Group", group).Msg("path now has owner/group")
	if err := db.InsertDir(path.Clean().String(), mode, pathInfo.ModTime().UnixNano(), owner, group, aclstring); err != nil {
		return err
	}
	return nil
}

func changeObjMode(ctx Context, newOwner, newGroup int, groupSet bool, path, repo *pathlib.Path, db pkg.DB) error {
	fullPath := repo.JoinPath(path)
	fullPathString := fullPath.Clean().String()
	pathInfo, err := os.Lstat(fullPathString)
	if err != nil {
		log.Error().Err(err).Str("Path", fullPathString).Msg("Failed in lstating file")
		return err
	}
	owner, group, mode, err := pkg.GetPathPerms(pathInfo)
	if err != nil {
		return err
	}
	ctx.cfg = pkg.GetBasePathPrefix(ctx.cfg, path)
	if ctx.cfg.Repo.CurrentGroupConfig.CheckPermissions && !verifyOwner(ctx, owner) {
		err := fmt.Errorf("permission error")
		log.Error().Err(err).Str("Path", path.Clean().String()).Msg("You do not have permissions to chmod on this path")
		return err
	}
	if !groupSet {
		newGroup = group
	}
	switch {
	case pathlib.IsDir(pathInfo.Mode()):
		log.Debug().Str("Path", fullPathString).Msg("Path is dir")
		if err := changeDirMode(ctx, newOwner, newGroup, mode, path, repo, pathInfo, db); err != nil {
			return err
		}
		if ctx.recursive {
			childPaths, err := pkg.ReadDirExclude(ctx.cfg, fullPath, false)
			if err != nil {
				return err
			}
			for _, child := range childPaths {
				if child.Name() != pkg.CVMFSProtectedFile && child.Name() != pkg.CVMFSAutoProtectedFile {
					if err := changeObjMode(ctx, newOwner, newGroup, groupSet, path.Join(child.Name()), repo, db); err != nil {
						return err
					}
				}
			}
		}
	case pathlib.IsFile(pathInfo.Mode()):
		log.Debug().Str("Path", fullPathString).Msg("Path is file")
		if !ctx.cfg.Repo.DotScheme {
			if err := changeFileMode(ctx, newOwner, newGroup, mode, path, repo, pathInfo, db); err != nil {
				return err
			}
		}
	case pathlib.IsSymlink(pathInfo.Mode()):
		log.Debug().Str("Path", fullPathString).Msg("Path is sym")
		if err := changeSymMode(ctx, newOwner, newGroup, mode, groupSet, path, repo, pathInfo, db); err != nil {
			return err
		}
	default:
		err := fmt.Errorf("unknown object type")
		log.Error().Err(err).Str("Path", fullPathString).Msg("Path is an unknown object type.")
		return err
	}
	return nil
}

func processChown(ctx Context, owner, group int, groupSet bool, paths []*pathlib.Path, repo *pathlib.Path, db pkg.DB) error {
	for _, path := range paths {
		if err := changeObjMode(ctx, owner, group, groupSet, path, repo, db); err != nil {
			return err
		}
	}
	return nil
}
