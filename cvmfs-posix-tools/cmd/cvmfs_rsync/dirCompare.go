package main

import (
	"fmt"

	"github.com/chigopher/pathlib"
	"github.com/rs/zerolog/log"
)

func compareDirPermissions(srcInfo, destInfo *DirInfo) (bool, error) {
	if srcInfo.oGM.owner != destInfo.oGM.owner || srcInfo.oGM.group != destInfo.oGM.group || uint16(srcInfo.oGM.mode) != uint16(destInfo.oGM.mode) || srcInfo.aclString != destInfo.aclString {
		return false, nil
	}
	return true, nil
}

// returns if copy should proceed, and if underlying object should be deleted
func dirProceed(ctx Context, srcInfo *DirInfo, destInfo FileSystemObjectInfo) (bool, bool, []*pathlib.Path, error) {
	switch tDestInfo := destInfo.(type) {
	case *FileInfo:
		return true, true, nil, nil
	case *SymInfo:
		return true, true, nil, nil
	case *DotSchemeFileInfo:
		return true, true, nil, nil
	case *NonExistentInfo:
		return true, false, nil, nil
	case *DirInfo:
		dirsSame, err := compareDirPermissions(srcInfo, tDestInfo)
		if err != nil {
			return false, false, nil, err
		}
		deletePaths := []*pathlib.Path{}
		if ctx.delete {
			keepPaths := make(map[*pathlib.Path]struct{})
			for _, srcSrcDest := range *srcInfo.childPathsP {
				keepPaths[srcSrcDest.dest] = struct{}{}
			}
			for _, destSrcDest := range *tDestInfo.childPathsP {
				if _, contains := keepPaths[destSrcDest.dest]; !contains {
					deletePaths = append(deletePaths, destSrcDest.dest)
				}
			}
		}
		return !dirsSame, false, deletePaths, nil
	default:
		err := fmt.Errorf("dest is unknown type")
		log.Error().Err(err).Msg("Unknown dest")
		return false, false, nil, err
	}
}
