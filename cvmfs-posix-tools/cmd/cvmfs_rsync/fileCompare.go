package main

import (
	"fmt"
	"io/fs"

	pathlib "github.com/chigopher/pathlib"
	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
	"github.com/rs/zerolog/log"
)

// Get hash data for a file. Assumes the passed in path points to a file
func getFileData(ctx Context, pathStat fs.FileInfo, path *pathlib.Path, cvmfsLocation bool) (pkg.FileHashData, error) {
	var hashData pkg.FileHashData
	var err error
	if cvmfsLocation {
		hashData, err = ctx.filewalkHasher.HashFileFromXattrs(path, ctx.cfg.Repo.DotScheme)
		if err != nil {
			log.Error().Err(err).Msg("Error getting hash data from xattrs.")
		} else {
			return hashData, nil
		}
	}
	if ctx.cfg.Repo.ContentAddressable {
		hashData, err = ctx.filewalkHasher.HashFileWithCompression(pathStat, path, ctx.cvmfsChunkSize, ctx.numComputeFilewalkWorkers, ctx.filewalkCompressor)
	} else {
		hashData, err = ctx.filewalkHasher.HashFile(pathStat, path, ctx.cvmfsChunkSize)
	}
	if err != nil {
		log.Error().Err(err).Msg("Error getting file data")
		return pkg.FileHashData{}, err
	}
	return hashData, err
}

// Get hash data for a file if checksum or dryrun, otherwise return empty data
func getFileDataIfNeeded(ctx Context, pathStat fs.FileInfo, path *pathlib.Path, cvmfsLocation bool) (pkg.FileHashData, error) {
	var hashData pkg.FileHashData
	var err error
	if ctx.checksum || ctx.dryrun {
		hashData, err = getFileData(ctx, pathStat, path, cvmfsLocation)
		if err != nil {
			return pkg.FileHashData{}, nil
		}
	}
	if err != nil {
		log.Error().Err(err).Str("Path", path.Clean().String()).Msg("Unable to hash path")
		return hashData, err
	}
	return hashData, nil
}

// Compare the mtime and size of a file, return true if they are the same
func compareFilesMtimeSize(srcInfo, destInfo *FileInfo) (bool, error) {
	srcStat := srcInfo.info
	destStat := destInfo.info
	if srcStat.ModTime().Unix() == destStat.ModTime().Unix() && srcStat.Size() == destStat.Size() {
		log.Debug().Msg("Same Mtime and Size")
		return true, nil
	}
	return false, nil
}

// Compare the mtime and size of a file and dotscheme file, return true if they are the same
func compareFileDotFileMtimeSize(srcInfo *FileInfo, destInfo *DotSchemeFileInfo) (bool, error) {
	srcStat := srcInfo.info
	destStat := destInfo.info
	if srcStat.ModTime().Unix() == destStat.ModTime().Unix() && srcStat.Size() == destStat.Size() {
		log.Debug().Msg("Same Mtime and Size")
		return true, nil
	}
	return false, nil
}

// Returns if the files are the same and the hashdata if ctx indicates it's necessary (dryrun or checksum)
func fileFileCompare(ctx Context, srcInfo, destInfo *FileInfo, srcPath, destPath *pathlib.Path) (bool, pkg.FileHashData, error) {
	var srcHashData pkg.FileHashData
	var err error
	if !ctx.checksum {
		filesSame, err := compareFilesMtimeSize(srcInfo, destInfo)
		if err != nil {
			return false, srcHashData, err
		}
		srcHashData, err = getFileDataIfNeeded(ctx, srcInfo.info, srcPath, false)
		if err != nil {
			return false, srcHashData, err
		}
		return filesSame, srcHashData, err
	} else {
		srcHashData, err = getFileData(ctx, srcInfo.info, srcPath, false)
		if err != nil {
			return false, srcHashData, err
		}
		destHashData, err := getFileData(ctx, destInfo.info, destPath, true)
		if err != nil {
			return false, srcHashData, err
		}
		if fmt.Sprintf("%040x", srcHashData.Checksum) != fmt.Sprintf("%040x", destHashData.Checksum) {
			return false, srcHashData, nil
		}
	}
	return true, srcHashData, nil
}

// Returns if src and dest are the same
// getSrcHashData if specified (ctx.checksum or ctx.dryrun), returns srcHashData if the files are different
func fileDSFileCompare(ctx Context, srcInfo *FileInfo, destInfo *DotSchemeFileInfo, srcPath, destPath *pathlib.Path) (bool, pkg.FileHashData, error) {
	var srcHashData pkg.FileHashData
	if !ctx.checksum {
		filesSame, err := compareFileDotFileMtimeSize(srcInfo, destInfo)
		if err != nil {
			return false, srcHashData, err
		}
		srcHashData, err = getFileDataIfNeeded(ctx, srcInfo.info, srcPath, false)
		if err != nil {
			return false, srcHashData, err
		}
		return filesSame, srcHashData, nil
	} else {
		srcHashData, err := getFileData(ctx, srcInfo.info, srcPath, false)
		if err != nil {
			return false, pkg.FileHashData{}, err
		}
		destFilePath := destPath
		if ctx.cfg.Repo.DotScheme {
			destFilePath = destPath.Parent().Join(destInfo.currentDotFile)
		}
		destHashData, err := getFileData(ctx, destInfo.info, destFilePath, true)
		if err != nil {
			return false, pkg.FileHashData{}, err
		}
		return fmt.Sprintf("%040x", destHashData.Checksum) == fmt.Sprintf("%040x", srcHashData.Checksum), srcHashData, nil
	}
}

// Indicates if one should proceed with the file copy
// Returns if file copy should proceed, and if dest equivalent needs to be deleted
func fileProceed(ctx Context, srcInfo *FileInfo, destInfo FileSystemObjectInfo, srcPath, destPath *pathlib.Path) (bool, bool, pkg.FileHashData, error) {
	switch tDestInfo := destInfo.(type) {
	case *FileInfo:
		filesSame, srcHashData, err := fileFileCompare(ctx, srcInfo, tDestInfo, srcPath, destPath)
		if err != nil {
			return false, false, pkg.FileHashData{}, err
		}
		return !filesSame, false, srcHashData, nil
	case *DirInfo:
		// Can add checks for allowed permissions here
		if len(*tDestInfo.childPathsP) == 0 || ctx.delete {
			if ctx.delete && !(len(*tDestInfo.childPathsP) == 0) && !ctx.cfg.Repo.CurrentGroupConfig.AllowDelete {
				err := fmt.Errorf("configuration issue")
				log.Error().Err(err).Str("path", ctx.cfg.PathPrefix).Msg("Deleting from this directory is denied by configuration policy, Allow Delete is not true for this dir. Can't continue")
				return false, false, pkg.FileHashData{}, err
			}
			srcHashData, err := getFileDataIfNeeded(ctx, srcInfo.info, srcPath, false)
			if err != nil {
				return false, false, pkg.FileHashData{}, err
			}
			return true, true, srcHashData, nil
		} else {
			err := fmt.Errorf("trying to write file over full directory, exiting")
			log.Error().Err(err).Str("Destination", destPath.Clean().String()).Msg("If you wish to proceed with this action, please specify --delete")
			return false, false, pkg.FileHashData{}, err
		}
	case *SymInfo:
		srcHashData, err := getFileDataIfNeeded(ctx, srcInfo.info, srcPath, false)
		if err != nil {
			return false, false, pkg.FileHashData{}, err
		}
		return true, false, srcHashData, nil
	case *DotSchemeFileInfo:
		filesSame, srcHashData, err := fileDSFileCompare(ctx, srcInfo, tDestInfo, srcPath, destPath)
		if err != nil {
			return false, false, pkg.FileHashData{}, err
		}
		return !filesSame, false, srcHashData, nil
	case *NonExistentInfo:
		srcHashData, err := getFileDataIfNeeded(ctx, srcInfo.info, srcPath, false)
		if err != nil {
			return false, false, pkg.FileHashData{}, err
		}
		return true, false, srcHashData, nil
	default:
		err := fmt.Errorf("dest is unknown file type")
		log.Error().Err(err).Msg("Unknown dest")
		return false, false, pkg.FileHashData{}, err
	}
}

// Indicates if one should proceed with the file copy for ds files
// Returns if file copy should proceed, and if dest equivalent needs to be deleted
func dSFileProceed(ctx Context, srcInfo *DotSchemeFileInfo, destInfo FileSystemObjectInfo, srcPath, destPath *pathlib.Path) (bool, bool, error) {
	switch tDestInfo := destInfo.(type) {
	case *FileInfo:
		// This would be copying from cvmfs to local, so there is no need for a hash
		filesSame, _, err := fileDSFileCompare(ctx, tDestInfo, srcInfo, srcPath, destPath)
		if err != nil {
			return false, false, err
		}
		return !filesSame, false, nil
	case *DirInfo:
		// Can add checks for allowed permissions here
		if len(*tDestInfo.childPathsP) == 0 || ctx.delete {
			if ctx.delete && !(len(*tDestInfo.childPathsP) == 0) && !ctx.cfg.Repo.CurrentGroupConfig.AllowDelete {
				err := fmt.Errorf("configuration issue")
				log.Error().Err(err).Str("path", ctx.cfg.PathPrefix).Msg("Deleting from this directory is denied by configuration policy, Allow Delete is not true for this dir. Can't continue")
				return false, false, err
			}
			return true, true, nil
		} else {
			err := fmt.Errorf("trying to write file over full directory, exiting")
			log.Error().Err(err).Str("Destination", destPath.Clean().String()).Msg("If you wish to proceed with this action, please specify --delete")
			return false, false, err
		}
	case *SymInfo:
		return true, true, nil
	case *DotSchemeFileInfo:
		err := fmt.Errorf("impossible case")
		log.Error().Err(err).Msg("We should never be comparing a dot file to a dot file")
		return false, false, err
	case *NonExistentInfo:
		return true, false, nil
	default:
		err := fmt.Errorf("dest is unknown file type")
		log.Error().Err(err).Msg("Unknown dest")
		return false, false, err
	}
}
