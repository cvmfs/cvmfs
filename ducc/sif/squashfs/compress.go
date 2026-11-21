package squashfs

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/ulikunitz/xz"
	"github.com/ulikunitz/xz/lzma"
)

// decompressor decompresses one squashfs block into dst[:0], producing at
// most limit bytes; more is treated as corruption.
type decompressor interface {
	decompress(dst, src []byte, limit int) ([]byte, error)
}

func newDecompressor(c Compressor) (decompressor, error) {
	switch c {
	case CompGzip:
		return &zlibDecompressor{}, nil
	case CompXz:
		return streamDecompressor(func(r io.Reader) (io.Reader, error) { return xz.NewReader(r) }), nil
	case CompLzma:
		return streamDecompressor(func(r io.Reader) (io.Reader, error) { return lzma.NewReader(r) }), nil
	case CompLz4:
		return lz4Decompressor{}, nil
	case CompZstd:
		return newZstdDecompressor()
	}
	return nil, fmt.Errorf("unsupported squashfs compressor %s", c)
}

// readLimited reads into dst without growing it past limit, so pooled
// block buffers are reused instead of reallocated at twice the size.
func readLimited(dst []byte, r io.Reader, limit int) ([]byte, error) {
	if cap(dst) < limit {
		dst = make([]byte, limit)
	}
	n, err := io.ReadFull(r, dst[:limit])
	switch err {
	case io.EOF, io.ErrUnexpectedEOF:
		return dst[:n], nil
	case nil:
		// The extra read also lets zlib verify its trailing checksum.
		var one [1]byte
		m, rerr := r.Read(one[:])
		if m > 0 {
			return nil, fmt.Errorf("decompressed block exceeds %d bytes", limit)
		}
		if rerr != nil && rerr != io.EOF {
			return nil, rerr
		}
		return dst[:n], nil
	}
	return nil, err
}

type zlibDecompressor struct{ pool sync.Pool }

func (z *zlibDecompressor) decompress(dst, src []byte, limit int) ([]byte, error) {
	var zr io.ReadCloser
	var err error
	if p := z.pool.Get(); p != nil {
		zr = p.(io.ReadCloser)
		err = zr.(zlib.Resetter).Reset(bytes.NewReader(src), nil)
	} else {
		zr, err = zlib.NewReader(bytes.NewReader(src))
	}
	if err != nil {
		return nil, fmt.Errorf("gzip: %w", err)
	}
	out, err := readLimited(dst, zr, limit)
	if err == nil {
		z.pool.Put(zr)
	} else {
		err = fmt.Errorf("gzip: %w", err)
	}
	return out, err
}

type streamDecompressor func(io.Reader) (io.Reader, error)

func (s streamDecompressor) decompress(dst, src []byte, limit int) ([]byte, error) {
	r, err := s(bytes.NewReader(src))
	if err != nil {
		return nil, err
	}
	return readLimited(dst, r, limit)
}

type lz4Decompressor struct{}

func (lz4Decompressor) decompress(dst, src []byte, limit int) ([]byte, error) {
	if cap(dst) < limit {
		dst = make([]byte, limit)
	}
	n, err := lz4.UncompressBlock(src, dst[:limit])
	if err != nil {
		return nil, fmt.Errorf("lz4: %w", err)
	}
	return dst[:n], nil
}

type zstdDecompressor struct{ d *zstd.Decoder }

func newZstdDecompressor() (decompressor, error) {
	// DecodeAll is safe for concurrent use; the memory cap stops a hostile
	// frame header from allocating a huge window.
	d, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(0),
		zstd.WithDecoderMaxMemory(64<<20))
	if err != nil {
		return nil, err
	}
	return zstdDecompressor{d}, nil
}

func (z zstdDecompressor) decompress(dst, src []byte, limit int) ([]byte, error) {
	out, err := z.d.DecodeAll(src, dst[:0])
	if err != nil {
		return nil, fmt.Errorf("zstd: %w", err)
	}
	if len(out) > limit {
		return nil, fmt.Errorf("decompressed block exceeds %d bytes", limit)
	}
	return out, nil
}
