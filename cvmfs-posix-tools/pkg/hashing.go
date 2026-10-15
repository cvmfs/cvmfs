package pkg

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"strconv"
	"strings"
	"sync"
	"time"

	pathlib "github.com/chigopher/pathlib"
	"github.com/rs/zerolog/log"
)

type Hasher struct {
	hasherPool *sync.Pool
	bufferPool *sync.Pool
	maxHashers int
}

type FileHashData struct {
	Hashes   [][]byte
	Checksum []byte
}

type FileNumReader struct {
	chunkId int
	reader  NamedReader
}

type FileHashSegment struct {
	chunkId int
	hash    []byte
}

// Create a new hasher with num hashers and given buffer size
func NewHasher(maxHashers, bufferSize int) *Hasher {
	return &Hasher{
		hasherPool: &sync.Pool{
			New: func() any {
				return sha1.New()
			},
		},
		bufferPool: &sync.Pool{
			New: func() any {
				s := make([]byte, bufferSize)
				return &s
			},
		},
		maxHashers: maxHashers,
	}
}

// Convert a slice of hashes to a slice of strings
func HashesToStrings(hashes [][]byte) []string {
	var strHashes []string
	for _, hash := range hashes {
		strHashes = append(strHashes, fmt.Sprintf("%040x", hash))
	}
	return strHashes
}

// Process any errors encountered during hashing files
func processHashingErrors(errs <-chan error, pathString string) error {
	if len(errs) > 0 {
		for err := range errs {
			log.Error().Err(err).Msg("Received error while hashing")
		}
		err := fmt.Errorf("error while hashing file")
		log.Error().Err(err).Str("File", pathString).Msg("Error on file")
		return err
	}
	return nil
}

func (hasher *Hasher) HashEmptyFile(hs hash.Hash) FileHashData {
	h := hasher.hasherPool.Get().(hash.Hash)
	defer hasher.hasherPool.Put(h)
	h.Reset()
	hash := h.Sum(nil)
	hs.Write(hash)
	return FileHashData{[][]byte{hash}, hs.Sum(nil)}
}

// Get the checksum from hashes that are already ordered
func (hasher *Hasher) GetChecksumFromOrderedHashes(hashes [][]byte) []byte {
	h := hasher.hasherPool.Get().(hash.Hash)
	defer hasher.hasherPool.Put(h)
	h.Reset()
	for _, hash := range hashes {
		h.Write(hash)
	}
	res := h.Sum(nil)
	return res
}

// Order the file segment hashes and calculate the checksum from them
func (hasher *Hasher) OrderHashesAndHash(hashSegments <-chan FileHashSegment, numSegments int, hs hash.Hash) ([][]byte, []byte) {
	chunkHashes := make([][]byte, numSegments)
	for hashSegment := range hashSegments {
		chunkHashes[hashSegment.chunkId] = hashSegment.hash
	}
	for _, hash := range chunkHashes {
		hs.Write(hash)
	}
	return chunkHashes, hs.Sum(nil)
}

func (hasher *Hasher) GetMaxHashers() int {
	return hasher.maxHashers
}

// A worker that will take named readers from the passed channel and try to hash that section of a file, acknowledging compression.
func (hasher *Hasher) HashFromReaderWorker(reader FileNumReader, chunkId int, h hash.Hash, hashSegments chan<- FileHashSegment, errs chan<- error, pathString string, compressor *Compressor) {
	namedReader := reader.reader
	activeReader := namedReader.Reader
	if compressor != nil {
		var cleanupFunction func() error
		var err error
		activeReader, cleanupFunction, err = CompressSectionReader(namedReader.Reader, compressor)
		if err != nil {
			errs <- err
			return
		}
		defer func() {
			if err := cleanupFunction(); err != nil {
				log.Error().Err(err).Msg("Error cleaning up compression code")
				errs <- err
			}
		}()
	}
	if err := SeekStart(activeReader); err != nil {
		errs <- err
		return
	}
	h.Reset()
	localBuffer := hasher.bufferPool.Get().(*[]byte)
	log.Debug().Str("File", pathString).Str("ChunkId", strconv.Itoa(chunkId)).Msg("Hashing Section chunkId of file")
	if _, err := CopyBuffer(h, activeReader, *localBuffer); err != nil {
		log.Error().Err(err).Msg("Failed to copy data in hashing")
		hasher.bufferPool.Put(localBuffer)
		errs <- err
		return
	}
	hash := h.Sum(nil)
	hashSegments <- FileHashSegment{chunkId: chunkId, hash: hash}
	hasher.bufferPool.Put(localBuffer)
	log.Debug().Str("File", pathString).Str("ChunkId", strconv.Itoa(chunkId)).Msg("Finished hashing section chunkId")
}

// Returns hashes as bytes in chunk order as well as the checksum of all of the hashes in that order
func (hasher *Hasher) HashFileFromReaderList(pathString string, readers []NamedReader, chunkSize int64, compressor *Compressor) (fileHashData FileHashData, zeroLength bool, err error) {
	log.Debug().Str("File", pathString).Msg("Hashing File")
	start_time := time.Now()

	// Get num chunks to hash
	numReaders := len(readers)

	hs := hasher.hasherPool.Get().(hash.Hash)
	defer hasher.hasherPool.Put(hs)
	hs.Reset()
	// Hash empty file
	if numReaders == 0 {
		return hasher.HashEmptyFile(hs), true, nil
	}

	// Set up and spawn hash segment workers
	var wg sync.WaitGroup
	fileNumReaders := make(chan FileNumReader)
	hashSegments := make(chan FileHashSegment, numReaders)
	errs := make(chan error, numReaders)

	t := numReaders
	if t > hasher.maxHashers {
		t = hasher.maxHashers
	}

	for i := 0; i < t; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// COMPRESSIONCHECK
			// Would want to compress here, probably have worker do cleanup?
			h := hasher.hasherPool.Get().(hash.Hash)
			defer hasher.hasherPool.Put(h)
			for reader := range fileNumReaders {
				hasher.HashFromReaderWorker(reader, reader.chunkId, h, hashSegments, errs, pathString, compressor)
			}
		}()
	}

	for i, reader := range readers {
		// Probably change this to carry a namedreader
		fileNumReaders <- FileNumReader{chunkId: i, reader: reader}
	}
	close(fileNumReaders)
	wg.Wait()
	close(errs)
	close(hashSegments)

	// Process any errors
	if err := processHashingErrors(errs, pathString); err != nil {
		return FileHashData{nil, nil}, false, err
	}

	// Order hashes and calculate total checksum
	chunkHashes, fileHash := hasher.OrderHashesAndHash(hashSegments, numReaders, hs)

	end_time := time.Now()
	log.Debug().Str("file", pathString).Float64("delta (s)", end_time.Sub(start_time).Seconds()).Msg("Hashed file")

	log.Debug().Str("Checksum", fmt.Sprintf("%040x", fileHash)).Str("First Hash", fmt.Sprintf("%040x", chunkHashes[0])).Msg("Testing logging")
	return FileHashData{chunkHashes, fileHash}, false, err

}

// A worker that will take chunk ids from the passed channel and try to hash that section of a file
func hashSegmentWorker(chunkIds <-chan int, hashSegments chan<- FileHashSegment, errs chan<- error, hasher *Hasher, f *pathlib.File, pathString string, chunkSize int64) {
	h := hasher.hasherPool.Get().(hash.Hash)
	defer hasher.hasherPool.Put(h)
	for chunkId := range chunkIds {
		h.Reset()
		localBuffer := hasher.bufferPool.Get().(*[]byte)
		log.Debug().Str("File", pathString).Str("ChunkId", strconv.Itoa(chunkId)).Msg("Hashing Section chunkId of file")
		data := io.NewSectionReader(f, int64(chunkId)*chunkSize, chunkSize) //CVMFSChunkSize
		if _, err := CopyBuffer(h, data, *localBuffer); err != nil {
			log.Error().Err(err).Msg("Failed to copy data in hashing")
			hasher.bufferPool.Put(localBuffer)
			errs <- err
		}
		hash := h.Sum(nil)
		hashSegments <- FileHashSegment{chunkId: chunkId, hash: hash}
		hasher.bufferPool.Put(localBuffer)
		log.Debug().Str("File", pathString).Str("ChunkId", strconv.Itoa(chunkId)).Msg("Finished hashing section chunkId")
	}
}

// Returns hashes as bytes in chunk order as well as the checksum of all of the hashes in that order. Designed for hashing a file uncompressed
func (hasher *Hasher) HashFile(pathStat fs.FileInfo, path *pathlib.Path, chunkSize int64) (fileHashData FileHashData, err error) {
	log.Debug().Str("File", path.Clean().String()).Msg("Hashing File")
	start_time := time.Now()

	// Get num chunks to hash
	numChunks := GetFileNumChunks(pathStat.Size(), chunkSize)

	hs := hasher.hasherPool.Get().(hash.Hash)
	defer hasher.hasherPool.Put(hs)
	hs.Reset()
	// Hash empty file
	if numChunks == 0 {
		return hasher.HashEmptyFile(hs), nil
	}

	// Open file for reading
	var f *pathlib.File
	f, err = path.Open()
	if err != nil {
		log.Error().Err(err).Str("File", path.Clean().String()).Msg("Failed to open file to hash")
		return FileHashData{nil, nil}, err
	}
	defer func() {
		if tempErr := f.Close(); tempErr != nil {
			log.Error().Err(tempErr).Msg("Error in cleanup hashing file")
			if err == nil {
				err = tempErr
			}
		}
	}()

	// Set up and spawn hash segment workers
	var wg sync.WaitGroup
	chunkIds := make(chan int)
	hashSegments := make(chan FileHashSegment, numChunks)
	errs := make(chan error, numChunks)

	t := numChunks
	if t > hasher.maxHashers {
		t = hasher.maxHashers
	}

	for i := 0; i < t; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			hashSegmentWorker(chunkIds, hashSegments, errs, hasher, f, path.Clean().String(), chunkSize)
		}()
	}

	for chunkId := 0; chunkId < numChunks; chunkId++ {
		chunkIds <- chunkId
	}
	close(chunkIds)
	wg.Wait()
	close(errs)
	close(hashSegments)

	// Process any errors
	if err := processHashingErrors(errs, path.Clean().String()); err != nil {
		return FileHashData{nil, nil}, err
	}

	// Order hashes and calculate total checksum
	chunkHashes, fileHash := hasher.OrderHashesAndHash(hashSegments, numChunks, hs)

	end_time := time.Now()
	log.Debug().Str("file", path.String()).Float64("delta (s)", end_time.Sub(start_time).Seconds()).Msg("Hashed file")

	log.Debug().Str("Checksum", fmt.Sprintf("%040x", fileHash)).Str("First Hash", fmt.Sprintf("%040x", chunkHashes[0])).Msg("Testing logging")
	return FileHashData{chunkHashes, fileHash}, err

}

// Returns hashes as bytes in chunk order as well as the checksum of all of the hashes in that order. Designed for hashing a file uncompressed
func (hasher *Hasher) HashFileWithCompression(pathStat fs.FileInfo, path *pathlib.Path, chunkSize int64, maxWorkers int, compressor *Compressor) (fileHashData FileHashData, err error) {
	log.Debug().Str("File", path.Clean().String()).Msg("Hashing File")
	start_time := time.Now()
	pathString := path.Clean().String()
	fileSize := pathStat.Size()

	sectionReaders, f, err := GetFileReaders(pathString, fileSize, chunkSize, maxWorkers)
	if err != nil {
		return FileHashData{}, err
	}

	hashData, _, err := hasher.HashFileFromReaderList(pathString, sectionReaders, chunkSize, compressor)
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
func (hasher *Hasher) HashFileFromXattrs(path *pathlib.Path, isDotscheme bool) (FileHashData, error) {
	log.Debug().Str("File", path.Clean().String()).Msg("Hashing File From Xattrs")
	var checksum []byte
	chunkHashHexes, err := GetChunkHashesFromXattrs(path)
	if err != nil {
		return FileHashData{}, err
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
		checksum = hasher.GetChecksumFromOrderedHashes(chunkHashHexes)
	}
	log.Debug().Str("Checksum", fmt.Sprintf("%040x", checksum)).Str("First Hash", fmt.Sprintf("%040x", chunkHashHexes[0])).Msg("Testing logging")
	return FileHashData{Hashes: chunkHashHexes, Checksum: checksum}, nil
}
