package squashfs

import (
	"fmt"
	"strings"
)

type Kind uint8

const (
	KindDir Kind = iota + 1
	KindFile
	KindSymlink
	KindBlock
	KindChar
	KindFifo
	KindSocket
)

func (k Kind) String() string {
	return [...]string{"?", "dir", "file", "symlink", "blockdev", "chardev", "fifo", "socket"}[k]
}

// Entry is one path in the image. Hardlinked paths share Inode.
type Entry struct {
	Parent int32 // index into Index.Entries; -1 for the root
	Name   string
	Kind   Kind
	Perm   uint16 // permission bits incl. setuid/setgid/sticky
	UID    uint32
	GID    uint32
	Mtime  uint32
	Inode  uint32
	Nlink  uint32

	Size        uint64
	BlocksStart uint64
	Blocks      []uint32 // on-disk size words of the full blocks
	Frag        uint32   // fragment index, or NoFragment
	FragOff     uint32

	Target  string // symlink target
	Rdev    uint32 // squashfs-encoded device number
	Xattrs  uint32 // number of extended attributes
	XattrID uint32 // valid if Xattrs > 0
}

const NoFragment = noFragment

// Major/Minor decode the Linux new_encode_dev layout used by squashfs.
func (e *Entry) Major() int64 { return int64((e.Rdev & 0xfff00) >> 8) }
func (e *Entry) Minor() int64 { return int64((e.Rdev & 0xff) | ((e.Rdev >> 12) & 0xfff00)) }

// TailSize is the number of bytes stored in the fragment.
func (e *Entry) TailSize(blockSize int) int {
	if e.Frag == noFragment {
		return 0
	}
	return int(e.Size - uint64(len(e.Blocks))*uint64(blockSize))
}

type Index struct {
	Entries []Entry
	dirPath []string // indexed like Entries; set for directories only
}

// Path returns the image-relative path of entry i, without leading slash;
// the root is "".
func (ix *Index) Path(i int) string {
	e := &ix.Entries[i]
	if e.Kind == KindDir {
		return ix.dirPath[i]
	}
	return joinPath(ix.dirPath[e.Parent], e.Name)
}

// NewIndex starts an index assembled by the caller rather than read from an
// image, e.g. several layers merged into one tree.
func NewIndex(root Entry) *Index {
	root.Parent = -1
	return &Index{Entries: []Entry{root}, dirPath: []string{""}}
}

// Add appends e below e.Parent, which must be a directory already in ix, so
// parents keep preceding their children.
func (ix *Index) Add(e Entry) (int32, error) {
	if e.Parent < 0 || int(e.Parent) >= len(ix.Entries) || ix.Entries[e.Parent].Kind != KindDir {
		return 0, fmt.Errorf("%s: parent %d is not a directory in the index", e.Name, e.Parent)
	}
	if e.Name == "" || e.Name == "." || e.Name == ".." || strings.ContainsAny(e.Name, "/\x00") {
		return 0, fmt.Errorf("invalid name %q", e.Name)
	}
	ix.Entries = append(ix.Entries, e)
	p := ""
	if e.Kind == KindDir {
		p = joinPath(ix.dirPath[e.Parent], e.Name)
	}
	ix.dirPath = append(ix.dirPath, p)
	return int32(len(ix.Entries) - 1), nil
}

func joinPath(dir, name string) string {
	if dir == "" {
		return name
	}
	return dir + "/" + name
}

type inodeHeader struct {
	Type     uint16
	Perm     uint16
	UIDIdx   uint16
	GIDIdx   uint16
	Mtime    uint32
	InodeNum uint32
}

type dirLoc struct {
	block uint32
	off   uint16
	size  uint32
}

const (
	maxNameLen   = 256
	maxTargetLen = 4096
)

// readInode parses the inode at ref into e; for directories it also returns
// the listing location.
func (fs *FS) readInode(ref uint64, e *Entry) (dirLoc, error) {
	var loc dirLoc
	block := fs.sb.InodeTableStart + ref>>16
	off := int(ref & 0xffff)
	if block >= fs.sb.DirTableStart || off >= metadataBlockSize {
		return loc, fmt.Errorf("inode reference %#x out of range", ref)
	}
	c := fs.cursor(block, off, fs.sb.DirTableStart)
	var h inodeHeader
	if err := c.readStruct(&h); err != nil {
		return loc, fmt.Errorf("inode %#x: %w", ref, err)
	}
	var err error
	if e.UID, err = fs.id(h.UIDIdx); err != nil {
		return loc, err
	}
	if e.GID, err = fs.id(h.GIDIdx); err != nil {
		return loc, err
	}
	e.Perm = h.Perm & 0o7777
	e.Mtime = h.Mtime
	e.Inode = h.InodeNum
	e.Frag = noFragment
	xattr := uint32(noXattr)

	switch h.Type {
	case 1:
		var d struct {
			BlockIndex, LinkCount uint32
			FileSize, BlockOffset uint16
			Parent                uint32
		}
		err = c.readStruct(&d)
		e.Kind, e.Nlink = KindDir, d.LinkCount
		loc = dirLoc{d.BlockIndex, d.BlockOffset, uint32(d.FileSize)}
	case 8:
		var d struct {
			LinkCount, FileSize, BlockIndex, Parent uint32
			IndexCount, BlockOffset                 uint16
			Xattr                                   uint32
		}
		err = c.readStruct(&d)
		e.Kind, e.Nlink, xattr = KindDir, d.LinkCount, d.Xattr
		loc = dirLoc{d.BlockIndex, d.BlockOffset, d.FileSize}
	case 2:
		var f struct{ BlocksStart, Frag, FragOff, FileSize uint32 }
		if err = c.readStruct(&f); err == nil {
			e.Kind, e.Nlink = KindFile, 1
			e.BlocksStart, e.Frag, e.FragOff, e.Size = uint64(f.BlocksStart), f.Frag, f.FragOff, uint64(f.FileSize)
			err = fs.readBlockList(c, e)
		}
	case 9:
		var f struct {
			BlocksStart, FileSize, Sparse   uint64
			LinkCount, Frag, FragOff, Xattr uint32
		}
		if err = c.readStruct(&f); err == nil {
			e.Kind, e.Nlink, xattr = KindFile, f.LinkCount, f.Xattr
			e.BlocksStart, e.Frag, e.FragOff, e.Size = f.BlocksStart, f.Frag, f.FragOff, f.FileSize
			err = fs.readBlockList(c, e)
		}
	case 3, 10:
		var s struct{ LinkCount, TargetSize uint32 }
		if err = c.readStruct(&s); err == nil {
			if s.TargetSize == 0 || s.TargetSize > maxTargetLen {
				return loc, fmt.Errorf("inode %#x: symlink target length %d", ref, s.TargetSize)
			}
			t := make([]byte, s.TargetSize)
			if err = c.read(t); err == nil && h.Type == 10 {
				xattr, err = c.u32()
			}
			e.Kind, e.Nlink, e.Target = KindSymlink, s.LinkCount, string(t)
		}
	case 4, 5, 11, 12:
		var d struct{ LinkCount, Rdev uint32 }
		if err = c.readStruct(&d); err == nil && h.Type > 7 {
			xattr, err = c.u32()
		}
		e.Kind = KindBlock
		if h.Type == 5 || h.Type == 12 {
			e.Kind = KindChar
		}
		e.Nlink, e.Rdev = d.LinkCount, d.Rdev
	case 6, 7, 13, 14:
		e.Nlink, err = c.u32()
		if err == nil && h.Type > 7 {
			xattr, err = c.u32()
		}
		e.Kind = KindFifo
		if h.Type == 7 || h.Type == 14 {
			e.Kind = KindSocket
		}
	default:
		return loc, fmt.Errorf("inode %#x: unknown type %d", ref, h.Type)
	}
	if err != nil {
		return loc, fmt.Errorf("inode %#x: %w", ref, err)
	}
	if xattr != noXattr {
		if int(xattr) >= len(fs.xattrs) {
			return loc, fmt.Errorf("inode %#x: xattr id %d out of range", ref, xattr)
		}
		e.Xattrs, e.XattrID = fs.xattrs[xattr].count, xattr
	}
	return loc, nil
}

func (fs *FS) readBlockList(c *mdCursor, e *Entry) error {
	bs := uint64(fs.sb.BlockSize)
	n := e.Size / bs
	if e.Frag == noFragment {
		n = (e.Size + bs - 1) / bs
	} else if int(e.Frag) >= len(fs.frags) {
		return fmt.Errorf("fragment index %d out of range", e.Frag)
	} else if e.FragOff+uint32(e.Size%bs) > fs.sb.BlockSize {
		return fmt.Errorf("fragment tail %d+%d exceeds block size", e.FragOff, e.Size%bs)
	}
	// The list is stored in the inode table, which bounds its length.
	if n*4 > fs.sb.DirTableStart-fs.sb.InodeTableStart {
		return fmt.Errorf("implausible block count %d for size %d", n, e.Size)
	}
	if n == 0 {
		return nil
	}
	raw := make([]byte, n*4)
	if err := c.read(raw); err != nil {
		return err
	}
	e.Blocks = make([]uint32, n)
	pos := e.BlocksStart
	for i := range e.Blocks {
		w := uint32(raw[i*4]) | uint32(raw[i*4+1])<<8 | uint32(raw[i*4+2])<<16 | uint32(raw[i*4+3])<<24
		if err := fs.checkDataWord(pos, w); err != nil {
			return fmt.Errorf("block %d: %w", i, err)
		}
		e.Blocks[i] = w
		pos += uint64(w & dataSizeMask)
	}
	return nil
}

func kindBasicType(k Kind) uint16 { return uint16(k) }

// BuildIndex walks the directory tree from the root. Parents always precede
// their children in the result. maxEntries bounds memory on hostile images.
func (fs *FS) BuildIndex(maxEntries int) (*Index, error) {
	ix := &Index{}
	var root Entry
	loc, err := fs.readInode(fs.sb.RootInodeRef, &root)
	if err != nil {
		return nil, fmt.Errorf("root inode: %w", err)
	}
	if root.Kind != KindDir {
		return nil, fmt.Errorf("root inode is a %s", root.Kind)
	}
	root.Parent = -1
	ix.Entries = append(ix.Entries, root)
	ix.dirPath = append(ix.dirPath, "")

	type pending struct {
		idx int
		loc dirLoc
	}
	stack := []pending{{0, loc}}
	seenDirs := map[uint32]bool{root.Inode: true}

	for len(stack) > 0 {
		p := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		children, err := fs.readDir(p.loc)
		if err != nil {
			return nil, fmt.Errorf("directory /%s: %w", ix.dirPath[p.idx], err)
		}
		var subdirs []pending
		for _, ch := range children {
			if len(ix.Entries) >= maxEntries {
				return nil, fmt.Errorf("image has more than %d entries", maxEntries)
			}
			e := Entry{Parent: int32(p.idx), Name: ch.name}
			cloc, err := fs.readInode(ch.ref, &e)
			if err != nil {
				return nil, fmt.Errorf("/%s: %w", joinPath(ix.dirPath[p.idx], ch.name), err)
			}
			if kindBasicType(e.Kind) != ch.typ {
				return nil, fmt.Errorf("/%s: directory entry type %d does not match inode type %s",
					joinPath(ix.dirPath[p.idx], ch.name), ch.typ, e.Kind)
			}
			idx := len(ix.Entries)
			ix.Entries = append(ix.Entries, e)
			if e.Kind == KindDir {
				if seenDirs[e.Inode] {
					return nil, fmt.Errorf("/%s: directory inode %d referenced twice",
						joinPath(ix.dirPath[p.idx], ch.name), e.Inode)
				}
				seenDirs[e.Inode] = true
				ix.dirPath = append(ix.dirPath, joinPath(ix.dirPath[p.idx], ch.name))
				subdirs = append(subdirs, pending{idx, cloc})
			} else {
				ix.dirPath = append(ix.dirPath, "")
			}
		}
		// Reverse push keeps siblings in listing (name) order.
		for i := len(subdirs) - 1; i >= 0; i-- {
			stack = append(stack, subdirs[i])
		}
	}
	return ix, nil
}

type dirChild struct {
	name string
	ref  uint64
	typ  uint16
}

func (fs *FS) readDir(loc dirLoc) ([]dirChild, error) {
	if loc.size < 3 {
		return nil, fmt.Errorf("invalid directory size %d", loc.size)
	}
	remaining := int(loc.size) - 3
	c := fs.cursor(fs.sb.DirTableStart+uint64(loc.block), int(loc.off), fs.sb.BytesUsed)
	var out []dirChild
	for remaining > 0 {
		var h struct{ Count, Start, InodeNum uint32 }
		if remaining < 12 {
			return nil, fmt.Errorf("truncated directory header")
		}
		if err := c.readStruct(&h); err != nil {
			return nil, err
		}
		remaining -= 12
		if h.Count >= 256 {
			return nil, fmt.Errorf("directory header count %d", h.Count+1)
		}
		for i := uint32(0); i <= h.Count; i++ {
			var de struct {
				Offset      uint16
				InodeOffset int16
				Type        uint16
				NameSize    uint16
			}
			if remaining < 8 {
				return nil, fmt.Errorf("truncated directory entry")
			}
			if err := c.readStruct(&de); err != nil {
				return nil, err
			}
			nlen := int(de.NameSize) + 1
			remaining -= 8 + nlen
			if nlen > maxNameLen || remaining < 0 {
				return nil, fmt.Errorf("invalid directory entry name length %d", nlen)
			}
			name := make([]byte, nlen)
			if err := c.read(name); err != nil {
				return nil, err
			}
			n := string(name)
			if n == "." || n == ".." || strings.ContainsAny(n, "/\x00") {
				return nil, fmt.Errorf("invalid name %q in directory", n)
			}
			if de.Type < 1 || de.Type > 7 {
				return nil, fmt.Errorf("entry %q: invalid type %d", n, de.Type)
			}
			out = append(out, dirChild{
				name: n,
				ref:  uint64(h.Start)<<16 | uint64(de.Offset),
				typ:  de.Type,
			})
		}
	}
	return out, nil
}
