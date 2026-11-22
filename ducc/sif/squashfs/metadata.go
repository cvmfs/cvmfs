package squashfs

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type mdBlock struct {
	data []byte
	next uint64 // absolute offset of the following metadata block
}

// readMetadataBlock returns the decompressed metadata block at absolute
// offset pos. Blocks are cached until DropMetadataCache.
func (fs *FS) readMetadataBlock(pos uint64) (*mdBlock, error) {
	fs.mdMu.Lock()
	b, ok := fs.mdCache[pos]
	fs.mdMu.Unlock()
	if ok {
		return b, nil
	}
	if pos+2 > fs.sb.BytesUsed {
		return nil, fmt.Errorf("metadata block at %d beyond image end", pos)
	}
	var hdr [2]byte
	if _, err := fs.r.ReadAt(hdr[:], int64(pos)); err != nil {
		return nil, fmt.Errorf("reading metadata header at %d: %w", pos, err)
	}
	h := binary.LittleEndian.Uint16(hdr[:])
	size := uint64(h &^ metadataUncompBit)
	if size == 0 || size > metadataBlockSize || pos+2+size > fs.sb.BytesUsed {
		return nil, fmt.Errorf("invalid metadata block size %d at %d", size, pos)
	}
	raw := make([]byte, size)
	if _, err := fs.r.ReadAt(raw, int64(pos+2)); err != nil {
		return nil, fmt.Errorf("reading metadata block at %d: %w", pos, err)
	}
	data := raw
	if h&metadataUncompBit == 0 {
		var err error
		data, err = fs.dec.decompress(nil, raw, metadataBlockSize)
		if err != nil {
			return nil, fmt.Errorf("metadata block at %d: %w", pos, err)
		}
	}
	b = &mdBlock{data: data, next: pos + 2 + size}
	fs.mdMu.Lock()
	fs.mdCache[pos] = b
	fs.mdMu.Unlock()
	return b, nil
}

// DropMetadataCache releases cached metadata blocks; call after the index
// has been built.
func (fs *FS) DropMetadataCache() {
	fs.mdMu.Lock()
	fs.mdCache = map[uint64]*mdBlock{}
	fs.mdMu.Unlock()
}

// mdCursor reads a byte stream that continues across metadata blocks.
type mdCursor struct {
	fs    *FS
	block uint64
	off   int
	limit uint64 // reading at or past this absolute block offset is an error
}

func (fs *FS) cursor(block uint64, off int, limit uint64) *mdCursor {
	return &mdCursor{fs: fs, block: block, off: off, limit: limit}
}

func (c *mdCursor) read(p []byte) error {
	for len(p) > 0 {
		if c.block >= c.limit {
			return fmt.Errorf("metadata read runs past table end")
		}
		b, err := c.fs.readMetadataBlock(c.block)
		if err != nil {
			return err
		}
		if c.off > len(b.data) {
			return fmt.Errorf("metadata offset %d beyond block of %d bytes", c.off, len(b.data))
		}
		n := copy(p, b.data[c.off:])
		p = p[n:]
		c.off += n
		if c.off == len(b.data) {
			c.block = b.next
			c.off = 0
		}
	}
	return nil
}

func (c *mdCursor) readStruct(v interface{}) error {
	buf := make([]byte, binary.Size(v))
	if err := c.read(buf); err != nil {
		return err
	}
	return binary.Read(bytes.NewReader(buf), binary.LittleEndian, v)
}

func (c *mdCursor) u32() (uint32, error) {
	var b [4]byte
	if err := c.read(b[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b[:]), nil
}

func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }

// readLookupTable reads a two-level table: an array of u64 pointers at start,
// each pointing to a metadata block, holding count entries of entrySize.
func (fs *FS) readLookupTable(start uint64, count, entrySize int) ([]byte, error) {
	if count == 0 {
		return nil, nil
	}
	total := count * entrySize
	nblocks := (total + metadataBlockSize - 1) / metadataBlockSize
	if start+uint64(nblocks)*8 > fs.sb.BytesUsed {
		return nil, fmt.Errorf("lookup table at %d beyond image end", start)
	}
	ptrs := make([]byte, nblocks*8)
	if _, err := fs.r.ReadAt(ptrs, int64(start)); err != nil {
		return nil, fmt.Errorf("reading lookup table at %d: %w", start, err)
	}
	out := make([]byte, 0, total)
	for i := 0; i < nblocks; i++ {
		b, err := fs.readMetadataBlock(binary.LittleEndian.Uint64(ptrs[i*8:]))
		if err != nil {
			return nil, err
		}
		out = append(out, b.data...)
	}
	if len(out) < total {
		return nil, fmt.Errorf("lookup table at %d truncated: %d < %d bytes", start, len(out), total)
	}
	return out[:total], nil
}
