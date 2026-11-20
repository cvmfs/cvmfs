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

	ids     []uint32
	frags   []fragment
	xattrs  []xattrID
	xattrKV uint64 // start of the xattr key/value metadata blocks
}

type xattrID struct {
	ref   uint64 // (block offset in the kv table << 16) | offset
	count uint32 // number of key/value pairs
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
	fs.xattrKV = binary.LittleEndian.Uint64(hdr[:])
	if fs.xattrKV >= start {
		return fmt.Errorf("xattr table offset %d out of range", fs.xattrKV)
	}
	fs.xattrs = make([]xattrID, count)
	for i := range fs.xattrs {
		fs.xattrs[i] = xattrID{binary.LittleEndian.Uint64(raw[i*16:]), binary.LittleEndian.Uint32(raw[i*16+8:])}
	}
	return nil
}

type Xattr struct {
	Name  string
	Value []byte
}

const (
	maxXattrs        = 1024
	maxXattrNameLen  = 255
	maxXattrValueLen = 64 << 10
	xattrOutOfLine   = 0x100
)

var xattrPrefixes = [...]string{"user.", "trusted.", "security."}

// Xattrs reads the extended attributes of entry e. It reads metadata blocks
// that BuildIndex may not have touched, so it is meant for the few entries
// whose attributes matter (overlayfs opaque directories).
func (fs *FS) Xattrs(e *Entry) ([]Xattr, error) {
	if e.Xattrs == 0 {
		return nil, nil
	}
	x := fs.xattrs[e.XattrID]
	if x.count > maxXattrs {
		return nil, fmt.Errorf("xattr id %d: %d attributes", e.XattrID, x.count)
	}
	limit := fs.sb.XattrIDTableStart
	c := fs.cursor(fs.xattrKV+x.ref>>16, int(x.ref&0xffff), limit)
	out := make([]Xattr, 0, x.count)
	for i := uint32(0); i < x.count; i++ {
		var k struct{ Type, NameSize uint16 }
		if err := c.readStruct(&k); err != nil {
			return nil, err
		}
		prefix := k.Type &^ xattrOutOfLine
		if int(prefix) >= len(xattrPrefixes) || k.NameSize > maxXattrNameLen {
			return nil, fmt.Errorf("xattr id %d: invalid key type %d / length %d", e.XattrID, k.Type, k.NameSize)
		}
		name := make([]byte, k.NameSize)
		if err := c.read(name); err != nil {
			return nil, err
		}
		v, err := readXattrValue(c)
		if err != nil {
			return nil, err
		}
		if k.Type&xattrOutOfLine != 0 {
			if len(v) != 8 {
				return nil, fmt.Errorf("xattr id %d: out-of-line reference of %d bytes", e.XattrID, len(v))
			}
			ref := binary.LittleEndian.Uint64(v)
			if v, err = readXattrValue(fs.cursor(fs.xattrKV+ref>>16, int(ref&0xffff), limit)); err != nil {
				return nil, err
			}
		}
		out = append(out, Xattr{xattrPrefixes[prefix] + string(name), v})
	}
	return out, nil
}

func readXattrValue(c *mdCursor) ([]byte, error) {
	n, err := c.u32()
	if err != nil {
		return nil, err
	}
	if n > maxXattrValueLen {
		return nil, fmt.Errorf("xattr value of %d bytes", n)
	}
	v := make([]byte, n)
	return v, c.read(v)
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
