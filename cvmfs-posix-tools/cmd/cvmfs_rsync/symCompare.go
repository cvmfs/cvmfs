package main

import (
	"fmt"

	"github.com/rs/zerolog/log"
)

// Returns if one should proceed, and if the existing object needs to be deleted
func symProceed(ctx Context, srcInfo *SymInfo, destInfo FileSystemObjectInfo) (bool, bool, error) {
	switch tDestInfo := destInfo.(type) {
	case *DirInfo:
		if len(*tDestInfo.childPathsP) == 0 || ctx.delete {
			if ctx.delete && !(len(*tDestInfo.childPathsP) == 0) && !ctx.cfg.Repo.CurrentGroupConfig.AllowDelete {
				err := fmt.Errorf("configuration issue")
				log.Error().Err(err).Str("path", ctx.cfg.PathPrefix).Msg("Deleting from this directory is denied by configuration policy, Allow Delete is not true for this dir. Can't continue")
				return false, false, err
			}
			return true, true, nil
		} else {
			err := fmt.Errorf("trying to overwrite path")
			log.Error().Err(err).Str("Path", tDestInfo.info.Name()).Msg("Trying to overwrite full directory. Specify --delete to continue.")
			return false, false, err
		}
	case *FileInfo:
		return true, true, nil
	case *DotSchemeFileInfo:
		// Will need to delete something here probably
		// Only delete when delete specified
		return true, false, nil
	case *SymInfo:
		return srcInfo.target != tDestInfo.target, false, nil
	case *NonExistentInfo:
		return true, false, nil
	default:
		err := fmt.Errorf("dest is unknown type")
		log.Error().Err(err).Msg("Unknown dest")
		return false, false, err
	}
}
