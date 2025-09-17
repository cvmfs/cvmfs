package main

import (
	"fmt"
	"io/fs"
	"os"

	acl "github.com/cvmfs/cvmfs/cvmfs-posix-tools/go-acl"
	pathlib "github.com/chigopher/pathlib"
	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
	"github.com/rs/zerolog/log"
)

func verifyOwner(ctx Context, fileOwner int) bool {
	return ctx.uid == fileOwner
}

func getModdedAclString(ctx Context, fullPathString, aclModString string) (string, error) {
	dirAcl, err := acl.GetFileAccess(fullPathString)
	if err != nil {
		return "", err
	}
	defer dirAcl.Free()

	aclMod, err := acl.Parse(aclModString)
	if err != nil {
		return "", err
	}
	defer aclMod.Free()

	// Code here mostly taken from https://go-acl/blob/master/setfacl/setfacl.go
	if ctx.modifySet {
		for entry := aclMod.FirstEntry(); entry != nil; entry = aclMod.NextEntry() {
			log.Debug().Msg("Finding acl entry to modify (if any)")
			exEntry, err := pkg.FindAclEntry(entry, dirAcl)
			if err != nil {
				log.Error().Err(err).Str("Mod String", aclModString).Msg("Error finding an acl entry to mod")
				return "", err
			}
			if exEntry != nil {
				exQual, err := exEntry.GetQualifier()
				if err != nil {
					log.Debug().Msg("Replacing entry (unknown id)")
				} else {
					log.Debug().Int("Entry id", exQual).Msg("Replacing entry")
				}
				if err := dirAcl.DeleteEntry(exEntry); err != nil {
					log.Error().Err(err).Str("Mod String", aclModString).Msg("Error replacing acl")
					return "", err
				}
			}
			if err := dirAcl.AddEntry(entry); err != nil {
				log.Printf("Error copying entry (%s)\n", err)
			}
		}
	} else if ctx.removeSet {
		// We run this with the assumption that acls only have one of each entry. (Currently enforced by swissknife).
		for delEntry := aclMod.FirstEntry(); delEntry != nil; delEntry = aclMod.NextEntry() {
			exEntry, err := pkg.FindAclEntry(delEntry, dirAcl)
			if err != nil {
				log.Error().Err(err).Str("Mod String", aclModString).Msg("Error finding an acl entry to delete")
				return "", err
			}
			if exEntry != nil {
				if err := dirAcl.DeleteEntry(exEntry); err != nil {
					return "", err
				}
			}
		}
	} else if ctx.removeAll {
		for exEntry := dirAcl.FirstEntry(); exEntry != nil; exEntry = dirAcl.NextEntry() {
			_, err := exEntry.GetQualifier()
			if err != nil {
				continue
			}
			if err := dirAcl.DeleteEntry(exEntry); err != nil {
				return "", err
			}
		}
	}
	dirAcl.CalcMask()
	return dirAcl.String(), nil
}

func changeDirAcl(ctx Context, newAcl string, path, repo *pathlib.Path, pathInfo fs.FileInfo, db pkg.DB) error {
	owner, group, _, err := pkg.GetPathPerms(pathInfo)
	if err != nil {
		return err
	}
	parsedACL, err := acl.Parse(newAcl)
	if err != nil {
		log.Error().Err(err).Msg("Failed to parse ACL. Maybe you have an incorrect group?")
		return err
	}
	defer parsedACL.Free()
	_, mode, err := parsedACL.EquivMode()
	if err != nil {
		log.Error().Err(err).Msg("Failed to compute mode bits from ACL")
		return err
	}
	newAclNumeric := parsedACL.StringWithOptions(acl.TextNumericIDs)
	ctx.cfg = pkg.GetBasePathPrefix(ctx.cfg, path)
	if ctx.cfg.Repo.CurrentGroupConfig.CheckPermissions && !verifyOwner(ctx, owner) {
		err := fmt.Errorf("permission error")
		log.Error().Err(err).Str("Path", path.Clean().String()).Msg("You do not have permissions to setfacl on this path")
		return err
	}
	fullPath := repo.JoinPath(path)
	fullPathString := fullPath.Clean().String()
	log.Info().Str("Path", fullPathString).Msg("Changing dir")
	log.Debug().Str("Path", fullPathString).Str("Acl", newAclNumeric).Str("Mode", fmt.Sprintf("%o", mode)).Msg("path now has acl")
	if err := db.InsertDir(path.Clean().String(), int(mode), pathInfo.ModTime().UnixNano(), owner, group, newAclNumeric); err != nil {
		return err
	}
	return nil
}

func changeObjAcl(ctx Context, newAcl string, path, repo *pathlib.Path, db pkg.DB) error {
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
		if err := changeDirAcl(ctx, newAcl, path, repo, pathInfo, db); err != nil {
			return err
		}
		if ctx.recursive {
			childPaths, err := pkg.ReadDirExclude(ctx.cfg, fullPath, false)
			if err != nil {
				return err
			}
			for _, child := range childPaths {
				if child.Name() != pkg.CVMFSProtectedFile && child.Name() != pkg.CVMFSAutoProtectedFile {
					if err := changeObjAcl(ctx, newAcl, path.Join(child.Name()), repo, db); err != nil {
						return err
					}
				}
			}
		}
	default:
		log.Debug().Str("Path", fullPathString).Msg("non-dir object type, skipping")
	}
	return nil
}

func modObjAcl(ctx Context, aclModString string, path, repo *pathlib.Path, db pkg.DB) error {
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

		aclString, err := getModdedAclString(ctx, fullPathString, aclModString)
		if err != nil {
			log.Error().Err(err).Msg("Error getting modded acl string")
			return err
		}

		if err := changeDirAcl(ctx, aclString, path, repo, pathInfo, db); err != nil {
			return err
		}
		if ctx.recursive {
			childPaths, err := pkg.ReadDirExclude(ctx.cfg, fullPath, false)
			if err != nil {
				return err
			}
			for _, child := range childPaths {
				if child.Name() != pkg.CVMFSProtectedFile && child.Name() != pkg.CVMFSAutoProtectedFile {
					if err := modObjAcl(ctx, aclModString, path.Join(child.Name()), repo, db); err != nil {
						return err
					}
				}
			}
		}
	default:
		log.Debug().Str("Path", fullPathString).Msg("non-dir object type, skipping")
	}
	return nil
}

func processNewSetfacl(ctx Context, newAcl string, paths []*pathlib.Path, repo *pathlib.Path, db pkg.DB) error {
	for _, path := range paths {
		if err := changeObjAcl(ctx, newAcl, path, repo, db); err != nil {
			return err
		}
	}
	return nil
}

func processModSetfacl(ctx Context, aclModString string, paths []*pathlib.Path, repo *pathlib.Path, db pkg.DB) error {
	for _, path := range paths {
		if err := modObjAcl(ctx, aclModString, path, repo, db); err != nil {
			return err
		}
	}
	return nil
}
