package lib

import (
	"io"
	"os"
	"sync"

	temp "github.com/cvmfs/ducc/temp"
)

type DiskBufferReadAndHash struct {
	*ReadAndHash
}

type diskBuffer struct {
	wait  *sync.WaitGroup
	f     *os.File
	input io.ReadCloser
}

func NewDiskBuffer(r io.ReadCloser) (io.ReadCloser, error) {
	f, err := temp.UserDefinedTempFile()
	if err != nil {
		os.RemoveAll(f.Name())
		return &diskBuffer{}, err
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, err = io.Copy(f, r); err != nil {
			os.RemoveAll(f.Name())
			f.Close()
		}
		if _, err := f.Seek(0, 0); err != nil {
			os.RemoveAll(f.Name())
			f.Close()
		}
	}()
	db := diskBuffer{wait: &wg, f: f, input: r}
	return &db, nil
}

func (db *diskBuffer) Read(b []byte) (n int, err error) {
	db.wait.Wait()
	return db.f.Read(b)
}

func (db *diskBuffer) Close() error {
	// first we close the input, to force the goroutine to finish
	db.input.Close()
	// we are sure that the goroutine has finish
	db.wait.Wait()

	// now we clean up
	db.f.Close()
	os.RemoveAll(db.f.Name())
	return nil
}

func NewDiskBufferReadAndHash(r io.ReadCloser) (*DiskBufferReadAndHash, error) {
	d, err := NewDiskBuffer(r)
	if err != nil {
		return nil, err
	}
	readAndHash := NewReadAndHash(d)
	return &DiskBufferReadAndHash{readAndHash}, nil
}
