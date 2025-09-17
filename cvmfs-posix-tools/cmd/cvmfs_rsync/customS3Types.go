package main

import (
	"io"
	"sync"

	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type CustomLogger struct {
}

// Make new custom logger
func newCustomLogger() CustomLogger {
	return CustomLogger{}
}

// Log data
func (l CustomLogger) Log(args ...interface{}) {
	// Currently not logging anything from s3
}

type UnboundBufferedReadSeekerWriteToPool struct {
	readerPool sync.Pool
	writerPool sync.Pool
}

type UnboundBufferedReadSeekerWriteTo struct {
	*s3manager.BufferedReadSeeker
	localBuffer []byte
}

// Create new pool of unboundBufferedReadSeekerPool
func NewUnboundBufferedReadSeekerWriteToPool(readerSize, writerSize int) *UnboundBufferedReadSeekerWriteToPool {
	if readerSize < 65536 {
		readerSize = 65536
	}

	return &UnboundBufferedReadSeekerWriteToPool{
		readerPool: sync.Pool{New: func() interface{} {
			s := make([]byte, readerSize)
			return &s
		}},
		writerPool: sync.Pool{New: func() interface{} {
			s := make([]byte, writerSize)
			return &s
		}},
	}
}

// Write to with our own copy
func (b *UnboundBufferedReadSeekerWriteTo) WriteTo(writer io.Writer) (int64, error) {
	return pkg.CopyBuffer(writer, b.BufferedReadSeeker, b.localBuffer)
}

// Get a write to from the pool
func (p *UnboundBufferedReadSeekerWriteToPool) GetWriteTo(seeker io.ReadSeeker) (r s3manager.ReadSeekerWriteTo, cleanup func()) {
	buffer := p.readerPool.Get().(*[]byte)
	localBuffer := p.writerPool.Get().(*[]byte)

	r = &UnboundBufferedReadSeekerWriteTo{BufferedReadSeeker: s3manager.NewBufferedReadSeeker(seeker, *buffer), localBuffer: *localBuffer}
	cleanup = func() {
		p.writerPool.Put(localBuffer)
		p.readerPool.Put(buffer)
	}

	return r, cleanup
}
