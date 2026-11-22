// Package squashfs is a read-only squashfs 4.0 reader built for streaming
// conversion: it exposes inode-level data positions so callers can order
// reads by on-disk location, and does every read through an io.ReaderAt.
package squashfs

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	Magic          = 0x73717368
	superblockSize = 96

	metadataBlockSize = 8192
	metadataUncompBit = 0x8000
	dataUncompBit     = 1 << 24
	dataSizeMask      = dataUncompBit - 1
	noFragment        = 0xFFFFFFFF
	noXattr           = 0xFFFFFFFF
	maxBlockSize      = 1 << 20
	minBlockSize      = 4096
)

const (
	flagUncompressedInodes = 0x0001
	flagUncompressedData   = 0x0002
	flagUncompressedFrags  = 0x0008
	flagNoFragments        = 0x0010
	flagNoXattrs           = 0x0200
	flagCompressorOptions  = 0x0400
)

type Compressor uint16

const (
	CompGzip Compressor = 1
	CompLzma Compressor = 2
	CompLzo  Compressor = 3
	CompXz   Compressor = 4
	CompLz4  Compressor = 5
	CompZstd Compressor = 6
)

func (c Compressor) String() string {
	switch c {
	case CompGzip:
		return "gzip"
	case CompLzma:
		return "lzma"
	case CompLzo:
		return "lzo"
	case CompXz:
		return "xz"
	case CompLz4:
		return "lz4"
	case CompZstd:
		return "zstd"
	}
	return fmt.Sprintf("unknown(%d)", uint16(c))
}

type Superblock struct {
	Magic             uint32
	InodeCount        uint32
	ModTime           uint32
	BlockSize         uint32
	FragmentCount     uint32
	Compressor        Compressor
	BlockLog          uint16
	Flags             uint16
	IDCount           uint16
	VersionMajor      uint16
	VersionMinor      uint16
	RootInodeRef      uint64
	BytesUsed         uint64
	IDTableStart      uint64
	XattrIDTableStart uint64
	InodeTableStart   uint64
	DirTableStart     uint64
	FragTableStart    uint64
	ExportTableStart  uint64
}

func readSuperblock(r io.ReaderAt, size int64) (*Superblock, error) {
	buf := make([]byte, superblockSize)
	if _, err := r.ReadAt(buf, 0); err != nil {
		return nil, fmt.Errorf("reading squashfs superblock: %w", err)
	}
	var sb Superblock
	if err := binary.Read(bytesReader(buf), binary.LittleEndian, &sb); err != nil {
		return nil, err
	}
	if sb.Magic != Magic {
		return nil, fmt.Errorf("not a squashfs image (magic %#x)", sb.Magic)
	}
	if sb.VersionMajor != 4 || sb.VersionMinor != 0 {
		return nil, fmt.Errorf("unsupported squashfs version %d.%d", sb.VersionMajor, sb.VersionMinor)
	}
	if sb.BlockSize < minBlockSize || sb.BlockSize > maxBlockSize ||
		sb.BlockSize&(sb.BlockSize-1) != 0 || uint32(1)<<sb.BlockLog != sb.BlockSize {
		return nil, fmt.Errorf("invalid squashfs block size %d (log %d)", sb.BlockSize, sb.BlockLog)
	}
	if size > 0 && sb.BytesUsed > uint64(size) {
		return nil, fmt.Errorf("squashfs claims %d bytes, partition has %d", sb.BytesUsed, size)
	}
	for name, off := range map[string]uint64{
		"inode table": sb.InodeTableStart, "directory table": sb.DirTableStart,
		"id table": sb.IDTableStart,
	} {
		if off < superblockSize || off >= sb.BytesUsed {
			return nil, fmt.Errorf("squashfs %s offset %d out of range", name, off)
		}
	}
	if sb.InodeTableStart >= sb.DirTableStart {
		return nil, fmt.Errorf("squashfs inode table does not precede directory table")
	}
	return &sb, nil
}
