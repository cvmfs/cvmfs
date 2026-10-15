package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"syscall"

	acl "github.com/cvmfs/cvmfs/cvmfs-posix-tools/go-acl"
	pathlib "github.com/chigopher/pathlib"
	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
	"github.com/rs/zerolog/log"
	mode "github.com/tonistiigi/dchapes-mode"
)

func verifyOwner(ctx Context, fileOwner int) bool {
	return ctx.uid == fileOwner
}

func getModeChangeSet(ctx Context, modeStr string) (mode.Set, error) {
	var changeSet mode.Set
	var err error
	if ctx.referenceSet {
		fInfo, err := os.Stat(ctx.reference)
		if err != nil {
			log.Error().Err(err).Str("Path", ctx.reference).Msg("Failed in stating reference file")
			return mode.Set{}, err
		}
		_, _, fMode, err := pkg.GetPathPerms(fInfo)
		if err != nil {
			return mode.Set{}, err
		}
		changeSet, err = mode.Parse(strconv.FormatInt(int64(uint16(fMode)), 8))
		if err != nil {
			log.Error().Err(err).Msg("Failed in getting the mode from reference file")
			return mode.Set{}, err
		}
	} else {
		changeSet, err = mode.Parse(modeStr)
		if err != nil {
			log.Error().Err(err).Msg("Failed in getting the mode from passed in string")
			return mode.Set{}, err
		}
	}
	return changeSet, nil
}

func changeFileMode(ctx Context, changeSet mode.Set, path, repo *pathlib.Path, pathInfo fs.FileInfo, db pkg.DB) error {
	fullPath := repo.JoinPath(path)
	log.Info().Str("Path", fullPath.Clean().String()).Msg("Changing file")
	owner, group, _, err := pkg.GetPathPerms(pathInfo)
	if err != nil {
		return err
	}
	ctx.cfg = pkg.GetBasePathPrefix(ctx.cfg, path)
	if ctx.cfg.Repo.CurrentGroupConfig.CheckPermissions && !verifyOwner(ctx, owner) {
		err := fmt.Errorf("permission error")
		log.Error().Err(err).Str("Path", path.Clean().String()).Msg("You do not have permissions to chmod on this path")
		return err
	}
	newMode := changeSet.Apply(pathInfo.Mode())
	fHashData, err := ctx.hasher.HashFileFromXattrsWithFallback(pathInfo, fullPath, ctx.cvmfsChunkSize, ctx.numHashers, ctx.compressor, ctx.cfg.Repo.DotScheme)
	if err != nil {
		return err
	}
	log.Debug().Str("Path", fullPath.Clean().String()).Str("Mode", strconv.FormatInt(int64(uint16(newMode)), 8)).Msg("path now has mode")
	err = db.InsertFile(path.Clean().String(), "", int(newMode), pathInfo.ModTime().UnixNano(), owner, group,
		pathInfo.Size(), strings.Join(pkg.HashesToStrings(fHashData.Hashes), pkg.CommaSeparator), fmt.Sprintf("%040x", fHashData.Checksum), pathInfo, pkg.BoolToInt(ctx.cfg.Repo.ContentAddressable), pkg.IsAlternateBucketPath(ctx.cfg, path))
	if err != nil {
		return err
	}
	return nil
}

func changeSymMode(ctx Context, changeSet mode.Set, path, repo *pathlib.Path, pathInfo fs.FileInfo, db pkg.DB) error {
	fullPath := repo.JoinPath(path)
	fullPathString := fullPath.Clean().String()
	log.Info().Str("Path", fullPathString).Msg("Processing sym")
	if ctx.cfg.Repo.DotScheme {
		isDotLink, err := pkg.IsDotSchemeLink(repo.JoinPath(path))
		if err != nil {
			return err
		}
		if isDotLink {
			log.Info().Str("Path", fullPathString).Msg("Dot link sym, processing as file")
			fileName, err := os.Readlink(fullPathString)
			if err != nil {
				log.Error().Err(err).Str("Path", fullPathString).Msg("Unable to readlink of path")
				return err
			}
			filePathInfo, err := os.Stat(fullPathString)
			if err != nil {
				log.Error().Err(err).Str("Path", fullPathString).Msg("Failed in lstating file")
				return err
			}

			if err := changeFileMode(ctx, changeSet, path.Parent().Join(fileName), repo, filePathInfo, db); err != nil {
				return err
			}
		} else {
			log.Info().Str("Path", fullPathString).Msg("Skipping symlink, not dot")
		}
	} else {
		log.Info().Str("Path", fullPathString).Msg("Skipping symlink")
	}
	return nil
}

// update ACL_USER_OBJ/ACL_MASK/ACL_OTHER from mode, must only be called on a non-equivalent ACL
func updateACLFromMode(mode fs.FileMode, a *acl.ACL) {
	for e := a.FirstEntry(); e != nil; e = a.NextEntry() {
		tag, _ := e.GetTag()
		shiftBits := -1
		switch tag {
		case acl.TagUserObj:
			shiftBits = 6
		case acl.TagMask:
			shiftBits = 3
		case acl.TagOther:
			shiftBits = 0
		}
		if shiftBits != -1 {
			shiftedMode := mode >> shiftBits
			permset, _ := e.GetPermset()
			permset.ClearPerms()
			if shiftedMode&0o4 != 0 {
				permset.AddPerm(acl.PermRead)
			}
			if shiftedMode&0o2 != 0 {
				permset.AddPerm(acl.PermWrite)
			}
			if shiftedMode&0o1 != 0 {
				permset.AddPerm(acl.PermExecute)
			}
		}
	}
}

func changeDirMode(ctx Context, changeSet mode.Set, path, repo *pathlib.Path, pathInfo fs.FileInfo, db pkg.DB) error {
	owner, group, _, err := pkg.GetPathPerms(pathInfo)
	if err != nil {
		return err
	}
	ctx.cfg = pkg.GetBasePathPrefix(ctx.cfg, path)
	if ctx.cfg.Repo.CurrentGroupConfig.CheckPermissions && !verifyOwner(ctx, owner) {
		err := fmt.Errorf("permission error")
		log.Error().Err(err).Str("Path", path.Clean().String()).Msg("You do not have permissions to chmod on this path")
		return err
	}
	fullPath := repo.JoinPath(path)
	fullPathString := fullPath.Clean().String()
	log.Info().Str("Path", fullPathString).Msg("Changing dir")
	newMode := changeSet.Apply(pathInfo.Mode()) & os.ModePerm
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
			// update the ACL mask to match the mode
			updateACLFromMode(newMode, a)
			aclstring = a.StringWithOptions(acl.TextNumericIDs)
		}
	}
	log.Debug().Str("Path", fullPathString).Str("Mode", fmt.Sprintf("%o", newMode)).Str("Acl", aclstring).Msg("path now has mode")
	if err := db.InsertDir(path.Clean().String(), int(newMode), pathInfo.ModTime().UnixNano(), owner, group, aclstring); err != nil {
		return err
	}
	return nil
}

func changeObjMode(ctx Context, changeSet mode.Set, path, repo *pathlib.Path, db pkg.DB) error {
	fullPath := repo.JoinPath(path)
	fullPathString := fullPath.Clean().String()
	pathInfo, err := os.Lstat(fullPathString)
	if err != nil {
		log.Error().Err(err).Str("Path", fullPathString).Msg("Failed in lstating file")
		return err
	}
	switch {
	case pathlib.IsDir(pathInfo.Mode()):
		log.Debug().Str("Path", fullPathString).Msg("Path is dir")
		if err := changeDirMode(ctx, changeSet, path, repo, pathInfo, db); err != nil {
			return err
		}
		if ctx.recursive {
			childPaths, err := pkg.ReadDirExclude(ctx.cfg, fullPath, false)
			if err != nil {
				return err
			}
			for _, child := range childPaths {
				if child.Name() != pkg.CVMFSProtectedFile && child.Name() != pkg.CVMFSAutoProtectedFile {
					if err := changeObjMode(ctx, changeSet, path.Join(child.Name()), repo, db); err != nil {
						return err
					}
				}
			}
		}
	case pathlib.IsFile(pathInfo.Mode()):
		log.Debug().Str("Path", fullPathString).Msg("Path is file")
		if !ctx.cfg.Repo.DotScheme {
			if err := changeFileMode(ctx, changeSet, path, repo, pathInfo, db); err != nil {
				return err
			}
		}
	case pathlib.IsSymlink(pathInfo.Mode()):
		log.Debug().Str("Path", fullPathString).Msg("Path is sym")
		if err := changeSymMode(ctx, changeSet, path, repo, pathInfo, db); err != nil {
			return err
		}
	default:
		err := fmt.Errorf("unknown object type")
		log.Error().Err(err).Str("Path", fullPathString).Msg("Path is an unknown object type.")
		return err
	}
	return nil
}

func processChomd(ctx Context, changeSet mode.Set, paths []*pathlib.Path, repo *pathlib.Path, db pkg.DB) error {
	for _, path := range paths {
		if err := changeObjMode(ctx, changeSet, path, repo, db); err != nil {
			return err
		}
	}
	return nil
}
