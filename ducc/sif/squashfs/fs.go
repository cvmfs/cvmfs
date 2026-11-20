package squashfs

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

// FS is an opened squashfs image. Index building is single threaded; data
// block reads (ReadBlock) are safe for concurrent use.
type FS struct {
	r   io.ReaderAt
	sb  *Superblock
	dec decompressor

	mdMu    sync.Mutex
	mdCache map[uint64]*mdBlock

	ids        []uint32
	frags      []fragment
	xattrCount []uint32 // per xattr id: number of key/value pairs
}

type fragment struct {
	start uint64
	word  uint32
}

// Open reads the superblock and the small lookup tables (ids, fragments,
// xattr ids). size is the partition size, used for bounds checks.
func Open(r io.ReaderAt, size int64) (*FS, error) {
	sb, err := readSuperblock(r, size)
	if err != nil {
		return nil, err
	}
	dec, err := newDecompressor(sb.Compressor)
	if err != nil {
		return nil, err
	}
	fs := &FS{r: r, sb: sb, dec: dec, mdCache: map[uint64]*mdBlock{}}

	raw, err := fs.readLookupTable(sb.IDTableStart, int(sb.IDCount), 4)
	if err != nil {
		return nil, fmt.Errorf("id table: %w", err)
	}
	fs.ids = make([]uint32, sb.IDCount)
	for i := range fs.ids {
		fs.ids[i] = binary.LittleEndian.Uint32(raw[i*4:])
	}

	if sb.FragmentCount > 0 {
		if sb.FragTableStart >= sb.BytesUsed {
			return nil, fmt.Errorf("fragment table offset %d out of range", sb.FragTableStart)
		}
		raw, err = fs.readLookupTable(sb.FragTableStart, int(sb.FragmentCount), 16)
		if err != nil {
			return nil, fmt.Errorf("fragment table: %w", err)
		}
		fs.frags = make([]fragment, sb.FragmentCount)
		for i := range fs.frags {
			e := raw[i*16:]
			fs.frags[i] = fragment{binary.LittleEndian.Uint64(e), binary.LittleEndian.Uint32(e[8:])}
			if err := fs.checkDataWord(fs.frags[i].start, fs.frags[i].word); err != nil {
				return nil, fmt.Errorf("fragment %d: %w", i, err)
			}
		}
	}

	if sb.Flags&flagNoXattrs == 0 && sb.XattrIDTableStart != ^uint64(0) {
		if err := fs.readXattrIDs(); err != nil {
			return nil, fmt.Errorf("xattr id table: %w", err)
		}
	}
	return fs, nil
}

func (fs *FS) readXattrIDs() error {
	start := fs.sb.XattrIDTableStart
	if start+16 > fs.sb.BytesUsed {
		return fmt.Errorf("offset %d out of range", start)
	}
	var hdr [16]byte
	if _, err := fs.r.ReadAt(hdr[:], int64(start)); err != nil {
		return err
	}
	count := binary.LittleEndian.Uint32(hdr[8:])
	if uint64(count)*16 > fs.sb.BytesUsed {
		return fmt.Errorf("implausible xattr id count %d", count)
	}
	raw, err := fs.readLookupTable(start+16, int(count), 16)
	if err != nil {
		return err
	}
	fs.xattrCount = make([]uint32, count)
	for i := range fs.xattrCount {
		fs.xattrCount[i] = binary.LittleEndian.Uint32(raw[i*16+8:])
	}
	return nil
}

func (fs *FS) Superblock() Superblock { return *fs.sb }
func (fs *FS) BlockSize() int         { return int(fs.sb.BlockSize) }

func (fs *FS) checkDataWord(pos uint64, word uint32) error {
	n := uint64(word & dataSizeMask)
	if n > uint64(fs.sb.BlockSize) {
		return fmt.Errorf("data block of %d bytes exceeds block size", n)
	}
	if pos+n > fs.sb.BytesUsed {
		return fmt.Errorf("data block at %d+%d beyond image end", pos, n)
	}
	return nil
}

// ReadBlock reads and decompresses one data block stored at pos with size
// word, expecting exactly want bytes (a sparse block yields zeros). dst is
// reused if large enough.
func (fs *FS) ReadBlock(dst []byte, pos uint64, word uint32, want int) ([]byte, error) {
	if want > int(fs.sb.BlockSize) || want < 0 {
		return nil, fmt.Errorf("invalid block length %d", want)
	}
	if word == 0 {
		if cap(dst) < want {
			dst = make([]byte, want)
		}
		dst = dst[:want]
		for i := range dst {
			dst[i] = 0
		}
		return dst, nil
	}
	if err := fs.checkDataWord(pos, word); err != nil {
		return nil, err
	}
	raw := make([]byte, word&dataSizeMask)
	if _, err := fs.r.ReadAt(raw, int64(pos)); err != nil {
		return nil, fmt.Errorf("reading data block at %d: %w", pos, err)
	}
	var out []byte
	if word&dataUncompBit != 0 {
		out = raw
	} else {
		var err error
		out, err = fs.dec.decompress(dst, raw, int(fs.sb.BlockSize))
		if err != nil {
			return nil, fmt.Errorf("data block at %d: %w", pos, err)
		}
	}
	if len(out) < want {
		return nil, fmt.Errorf("data block at %d: got %d bytes, want %d", pos, len(out), want)
	}
	return out[:want], nil
}

// Fragment returns the location of fragment block idx.
func (fs *FS) Fragment(idx uint32) (pos uint64, word uint32, err error) {
	if int(idx) >= len(fs.frags) {
		return 0, 0, fmt.Errorf("fragment index %d out of range (%d fragments)", idx, len(fs.frags))
	}
	f := fs.frags[idx]
	return f.start, f.word, nil
}

// ReadFragment returns the whole decompressed fragment block idx.
func (fs *FS) ReadFragment(idx uint32) ([]byte, error) {
	pos, word, err := fs.Fragment(idx)
	if err != nil {
		return nil, err
	}
	if word == 0 {
		return nil, fmt.Errorf("fragment %d has zero size", idx)
	}
	raw := make([]byte, word&dataSizeMask)
	if _, err := fs.r.ReadAt(raw, int64(pos)); err != nil {
		return nil, fmt.Errorf("reading fragment %d: %w", idx, err)
	}
	if word&dataUncompBit != 0 {
		return raw, nil
	}
	return fs.dec.decompress(nil, raw, int(fs.sb.BlockSize))
}

func (fs *FS) id(idx uint16) (uint32, error) {
	if int(idx) >= len(fs.ids) {
		return 0, fmt.Errorf("id index %d out of range (%d ids)", idx, len(fs.ids))
	}
	return fs.ids[idx], nil
}
