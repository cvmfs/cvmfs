package pkg

import (
	"encoding/hex"
	"io"
	"io/fs"
	"strings"
	"time"

	pathlib "github.com/chigopher/pathlib"
	"github.com/rs/zerolog/log"
)

type SimpleHasher interface {
	HashFile(pathStat fs.FileInfo, path *pathlib.Path) (fileHashData FileHashData, err error)
	HashFileFromXattrsWithFallback(pathStat fs.FileInfo, path *pathlib.Path, chunkSize int64, maxWorkers int, compressor *Compressor, isDotscheme bool) (FileHashData, error)
}

type Sha1Hasher struct {
	hasher             *Hasher
	chunkSize          int64
	contentAddressable bool
}

func GetSha1Hasher(maxHashers int, chunkSize int64, contentAddressable bool) SimpleHasher {
	return Sha1Hasher{hasher: NewHasher(maxHashers, IOBufferSize), chunkSize: chunkSize, contentAddressable: contentAddressable}
}

// Returns hashes as bytes in chunk order as well as the checksum of all of the hashes in that order. Designed for hashing a file uncompressed
func (hasher Sha1Hasher) HashFile(pathStat fs.FileInfo, path *pathlib.Path) (fileHashData FileHashData, err error) {
	log.Debug().Str("Path", path.Clean().String()).Msg("Hashing file")
	if hasher.contentAddressable {
		compressor := NewZlibCompressor(IOBufferSize)
		var readers []NamedReader
		var f io.ReadSeekCloser
		readers, f, err = GetFileReaders(path.Clean().String(), pathStat.Size(), hasher.chunkSize, hasher.hasher.GetMaxHashers())
		if err != nil {
			return
		}
		defer func() {
			if tempErr := f.Close(); tempErr != nil {
				log.Error().Err(tempErr).Msg("Error in cleanup")
				if err == nil {
					err = tempErr
				}
			}
		}()
		fileHashData, _, err = hasher.hasher.HashFileFromReaderList(path.Clean().String(), readers, hasher.chunkSize, compressor)
	} else {
		fileHashData, err = hasher.hasher.HashFile(pathStat, path, hasher.chunkSize)
	}
	return
}

// Returns hashes as bytes in chunk order as well as the checksum of all of the hashes in that order. Designed for hashing a file uncompressed
func (hasher Sha1Hasher) HashFileWithCompression(pathStat fs.FileInfo, path *pathlib.Path, chunkSize int64, maxWorkers int, compressor *Compressor) (fileHashData FileHashData, err error) {
	log.Debug().Str("File", path.Clean().String()).Msg("Hashing File")
	start_time := time.Now()
	pathString := path.Clean().String()
	fileSize := pathStat.Size()

	sectionReaders, f, err := GetFileReaders(pathString, fileSize, chunkSize, maxWorkers)
	if err != nil {
		return FileHashData{}, err
	}

	hashData, _, err := hasher.hasher.HashFileFromReaderList(pathString, sectionReaders, chunkSize, compressor)
	if err != nil {
		f.Close()
		return FileHashData{}, err
	}

	err = f.Close()
	if err != nil {
		log.Error().Err(err).Msg("Error in cleaning up section reader")
		return FileHashData{}, err
	}
	end_time := time.Now()
	log.Debug().Str("file", path.String()).Float64("delta (s)", end_time.Sub(start_time).Seconds()).Msg("Hashed file with compression")
	return hashData, nil

}

// Returns hashes as bytes in chunk order as well as the checksum of all of the hashes in that order from the file's xattrs
func (hasher Sha1Hasher) HashFileFromXattrsWithFallback(pathStat fs.FileInfo, path *pathlib.Path, chunkSize int64, maxWorkers int, compressor *Compressor, isDotscheme bool) (FileHashData, error) {
	log.Debug().Str("File", path.Clean().String()).Msg("Hashing File From Xattrs")
	var checksum []byte
	chunkHashHexes, err := GetChunkHashesFromXattrs(path)
	if err != nil {
		log.Error().Err(err).Msg("Error getting hashes from xattrs")
		if compressor != nil {
			return hasher.HashFileWithCompression(pathStat, path, chunkSize, maxWorkers, compressor)
		} else {
			return hasher.HashFile(pathStat, path)
		}
	}
	if isDotscheme {
		pathNameSlice := strings.Split(path.Name(), DotSchemeDelimeter)
		checksumString := pathNameSlice[len(pathNameSlice)-1]
		checksum, err = hex.DecodeString(checksumString)
		if err != nil {
			log.Error().Err(err).Str("Checksum", string(checksum)).Msg("Unable to decode checksum hash")
			return FileHashData{}, err
		}
	} else {
		checksum = hasher.hasher.GetChecksumFromOrderedHashes(chunkHashHexes)
	}
	return FileHashData{Hashes: chunkHashHexes, Checksum: checksum}, nil
}
