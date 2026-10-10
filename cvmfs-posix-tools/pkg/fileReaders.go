package pkg

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/rs/zerolog/log"
)

type Compressor struct {
	compressorPool   *sync.Pool
	bufferPool       *sync.Pool
	bufferReaderPool *sync.Pool
	copyBufferPool   *sync.Pool
}

type FileReadSegment struct {
	chunkId     int
	reader      io.ReadSeeker
	buffer      *bytes.Buffer
	startOffset int64
	partSize    int64
}

type NamedReader struct {
	Name        string
	Reader      io.ReadSeeker
	StartOffset int64
	PartSize    int64
	// Maybe can pass the compressor through internally
}

func NewZlibCompressor(ioBufferSize int) *Compressor {
	return &Compressor{
		compressorPool: &sync.Pool{
			New: func() any {
				return zlib.NewWriter(nil)
			},
		},
		bufferPool: &sync.Pool{
			New: func() any {
				return new(bytes.Buffer)
			},
		},
		bufferReaderPool: &sync.Pool{
			New: func() any {
				return bytes.NewReader(nil)
			},
		},
		copyBufferPool: &sync.Pool{
			New: func() any {
				s := make([]byte, ioBufferSize)
				return &s
			},
		},
	}
}

// Seek reader to its start
func SeekStart(r io.ReadSeeker) error {
	if i, err := r.Seek(0, io.SeekStart); err != nil || i != 0 {
		if err == nil {
			err = fmt.Errorf("couldn't seek to beginning of file")
		}
		log.Error().Err(err).Msg("Failed to seek to beginning of read seeker")
		return err
	}
	return nil
}

// Get the number of chunks for a file based on the size
func GetFileNumChunks(size, chunkSize int64) int { // CVMFSChunkSize
	return int((size + chunkSize - 1) / chunkSize)
}

// Gets a reader for the compressed version of the passed in reader's contents
// Pulls a reader from the bufferReaderPool, please put it back when done
func CompressSectionReader(sectionReader io.ReadSeeker, compressor *Compressor) (io.ReadSeeker, func() error, error) {
	if err := SeekStart(sectionReader); err != nil {
		log.Error().Err(err).Msg("Failed to seek to beginning of read seeker")
		return nil, nil, err
	}
	in := compressor.bufferPool.Get().(*bytes.Buffer)
	w := compressor.compressorPool.Get().(*zlib.Writer)
	defer compressor.compressorPool.Put(w)
	w.Reset(in)
	bCopySlice := compressor.copyBufferPool.Get().(*[]byte)
	defer compressor.copyBufferPool.Put(bCopySlice)
	_, err := CopyBuffer(w, sectionReader, *bCopySlice)
	if err != nil {
		if err := w.Close(); err != nil {
			log.Error().Err(err).Msg("Error closing compression writer")
		}
		log.Error().Err(err).Msg("Error in copying into compression writer")
		in.Reset()
		compressor.bufferPool.Put(in)
		compressor.compressorPool.Put(w)
		return nil, nil, err
	}
	if err = w.Close(); err != nil {
		log.Error().Err(err).Msg("Error closing compression writer")
		in.Reset()
		compressor.bufferPool.Put(in)
		compressor.compressorPool.Put(w)
		return nil, nil, err
	}
	bRead := compressor.bufferReaderPool.Get().(*bytes.Reader)
	bRead.Reset(in.Bytes())
	cleanupFunction := func() error {
		if bRead != nil {
			compressor.bufferReaderPool.Put(bRead)
		}

		if in != nil {
			in.Reset()
			compressor.bufferPool.Put(in)
		}
		return nil
	}
	return bRead, cleanupFunction, nil
}

// Return section readers and compressed readers for the file based on the given offset
// Note, these will be OPEN when you receive them, it is your job to close them
func GetFileSectionReader(f *os.File, startOffset, partSize int64) io.ReadSeeker {
	return io.NewSectionReader(f, startOffset, partSize)
}

func getFileSectionReaders(chunkIds <-chan int, sectionReaders chan<- FileReadSegment, f *os.File, fileSize, chunkSize int64, numChunks int) {
	for chunkId := range chunkIds {
		partSize := int64(chunkSize)
		if chunkId == numChunks-1 {
			partSize = int64(fileSize % chunkSize)
			if partSize == 0 {
				partSize = chunkSize
			}
		}
		startOffset := int64(chunkId) * chunkSize
		sectionReader := GetFileSectionReader(f, startOffset, partSize)
		sectionReaders <- FileReadSegment{chunkId: chunkId, reader: sectionReader, startOffset: startOffset, partSize: partSize}
	}
}

// NEED TO CLOSE FILE AND PUT BACK COMPRESSOR THINGS
// Returns readers, compressed readers, file reader, and cleanup function (Note: they may all be the same, so close with caution)
// Will this massively slow things down?
func GetFileReaders(pathString string, fileSize, chunkSize int64, maxWorkers int) ([]NamedReader, io.ReadSeekCloser, error) {
	numChunks := GetFileNumChunks(fileSize, chunkSize)
	f, err := os.Open(pathString)
	if err != nil {
		log.Error().Err(err).Str("File", pathString).Msg("Failed to open file to read")
		return nil, nil, err
	}

	readers := make([]NamedReader, numChunks)

	// Set up and spawn hash segment workers
	var wg sync.WaitGroup
	chunkIds := make(chan int)
	sectionReaders := make(chan FileReadSegment, numChunks)

	t := numChunks
	if t > maxWorkers {
		t = maxWorkers
	}

	for i := 0; i < t; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			getFileSectionReaders(chunkIds, sectionReaders, f, fileSize, chunkSize, numChunks)
		}()
	}

	for chunkId := 0; chunkId < numChunks; chunkId++ {
		chunkIds <- chunkId
	}
	close(chunkIds)
	wg.Wait()
	close(sectionReaders)

	// Order readers
	for sectionReader := range sectionReaders {
		readers[sectionReader.chunkId] = NamedReader{Name: pathString, Reader: sectionReader.reader, StartOffset: sectionReader.startOffset, PartSize: sectionReader.partSize}
	}
	return readers, f, err
}
