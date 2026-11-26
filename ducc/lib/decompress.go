package lib

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"io"

	"github.com/klauspost/compress/zstd"
)

var (
	gzipMagic = []byte{0x1f, 0x8b}
	zstdMagic = []byte{0x28, 0xb5, 0x2f, 0xfd}
)

// layerReader closes the decompressor and the underlying blob stream together.
type layerReader struct {
	io.Reader
	closers []func() error
}

func (r *layerReader) Close() error {
	var firstErr error
	for _, c := range r.closers {
		if err := c(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// decompressLayer returns the uncompressed tar stream of a layer blob.
// The format is sniffed from the magic bytes rather than taken from the
// manifest media type, so that gzip, zstd and uncompressed layers all work
// even when a registry labels them inaccurately.
func decompressLayer(blob io.ReadCloser) (io.ReadCloser, error) {
	br := bufio.NewReader(blob)
	magic, err := br.Peek(len(zstdMagic))
	if err != nil && err != io.EOF {
		return nil, err
	}

	switch {
	case bytes.HasPrefix(magic, gzipMagic):
		gr, err := gzip.NewReader(br)
		if err != nil {
			return nil, err
		}
		return &layerReader{Reader: gr, closers: []func() error{gr.Close, blob.Close}}, nil
	case bytes.Equal(magic, zstdMagic):
		zr, err := zstd.NewReader(br)
		if err != nil {
			return nil, err
		}
		closeZstd := func() error { zr.Close(); return nil }
		return &layerReader{Reader: zr, closers: []func() error{closeZstd, blob.Close}}, nil
	default:
		return &layerReader{Reader: br, closers: []func() error{blob.Close}}, nil
	}
}
