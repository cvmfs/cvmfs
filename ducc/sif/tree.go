package sif

import (
	"fmt"
	"os"
	"strings"

	"github.com/cvmfs/ducc/sif/squashfs"
	"github.com/cvmfs/ducc/singularity"
)

const maxLayers = 255

// Tree is what Stream emits: an index whose entries may come from several
// squashfs layers, plus files generated in memory.
type Tree struct {
	Index  *squashfs.Index
	Layers []*squashfs.FS
	layer  []uint8 // per entry; nil when there is a single layer
	inline map[int32][]byte
}

// SingleTree wraps one squashfs image.
func SingleTree(fs *squashfs.FS, ix *squashfs.Index) *Tree {
	return &Tree{Index: ix, Layers: []*squashfs.FS{fs}}
}

func (t *Tree) layerOf(i int32) int {
	if t.layer == nil {
		return 0
	}
	return int(t.layer[i])
}

func (t *Tree) fsOf(i int32) *squashfs.FS { return t.Layers[t.layerOf(i)] }

func (t *Tree) inlineData(i int32) ([]byte, bool) {
	d, ok := t.inline[i]
	return d, ok
}

type MergeStats struct {
	Layers    int `json:"layers"`
	Whiteouts int `json:"whiteouts"`
	Opaque    int `json:"opaque_dirs"`
	// Entries of lower layers hidden by whiteouts, opaque directories or
	// replacement in a higher layer.
	Hidden    int `json:"hidden"`
	Generated int `json:"generated"`
}

const (
	whiteoutPrefix = ".wh."
	opaqueMarker   = ".wh..wh..opq"
	opaqueXattr    = "trusted.overlay.opaque"
	// rootless overlayfs (userxattr mount option)
	opaqueUserXattr = "user.overlay.opaque"
)

// SingularityCE converts layer whiteouts to the overlayfs form, as apptainer
// and singularity mount the layers with overlayfs: a 0/0 character device
// and an opaque xattr on directories. The AUFS form of the OCI spec is
// accepted as well.
func isWhiteout(e *squashfs.Entry) bool {
	return (e.Kind == squashfs.KindChar && e.Rdev == 0) ||
		(strings.HasPrefix(e.Name, whiteoutPrefix) && e.Name != opaqueMarker)
}

func whiteoutTarget(e *squashfs.Entry) string {
	if e.Kind == squashfs.KindChar {
		return e.Name
	}
	return strings.TrimPrefix(e.Name, whiteoutPrefix)
}

func isOpaque(fs *squashfs.FS, e *squashfs.Entry) (bool, error) {
	if e.Xattrs == 0 {
		return false, nil
	}
	xs, err := fs.Xattrs(e)
	if err != nil {
		return false, err
	}
	for _, x := range xs {
		if (x.Name == opaqueXattr || x.Name == opaqueUserXattr) && string(x.Value) == "y" {
			return true, nil
		}
	}
	return false, nil
}

// builder merges entries in place; removed entries are marked dead and
// dropped by build.
type builder struct {
	entries []squashfs.Entry
	layer   []uint8
	dead    []bool
	// Children of a directory with an index below cutoff[dir] are hidden:
	// they predate the layer that made the directory opaque.
	cutoff map[int32]int32
	inline map[int32][]byte
	// first/next are per-directory child lists; kids caches a name lookup
	// for the directories that were looked into.
	first, next []int32
	kids        map[int32]map[string]int32
	st          MergeStats
	replaced    int
	initialDead int
}

func newBuilder(ix *squashfs.Index) *builder {
	n := len(ix.Entries)
	b := &builder{
		entries: append([]squashfs.Entry(nil), ix.Entries...),
		layer:   make([]uint8, n),
		dead:    make([]bool, n),
		cutoff:  map[int32]int32{},
		inline:  map[int32][]byte{},
		first:   make([]int32, n),
		next:    make([]int32, n),
		kids:    map[int32]map[string]int32{},
	}
	for i := range b.first {
		b.first[i] = -1
	}
	for i := n - 1; i > 0; i-- {
		p := b.entries[i].Parent
		b.next[i], b.first[p] = b.first[p], int32(i)
	}
	return b
}

func (b *builder) children(p int32) map[string]int32 {
	if m, ok := b.kids[p]; ok {
		return m
	}
	m := map[string]int32{}
	for c := b.first[p]; c >= 0; c = b.next[c] {
		if !b.dead[c] {
			m[b.entries[c].Name] = c
		}
	}
	b.kids[p] = m
	return m
}

// lookup returns the live child name of directory p, or -1.
func (b *builder) lookup(p int32, name string) int32 {
	c, ok := b.children(p)[name]
	if !ok || b.dead[c] {
		return -1
	}
	if cut, ok := b.cutoff[p]; ok && c < cut {
		return -1
	}
	return c
}

func (b *builder) add(e squashfs.Entry, layer uint8) int32 {
	i := int32(len(b.entries))
	b.entries = append(b.entries, e)
	b.layer = append(b.layer, layer)
	b.dead = append(b.dead, false)
	b.first = append(b.first, -1)
	b.next = append(b.next, b.first[e.Parent])
	b.first[e.Parent] = i
	if m, ok := b.kids[e.Parent]; ok {
		m[e.Name] = i
	}
	return i
}

func (b *builder) kill(i int32) {
	b.dead[i] = true
	if m, ok := b.kids[b.entries[i].Parent]; ok && m[b.entries[i].Name] == i {
		delete(m, b.entries[i].Name)
	}
}

// put places e (with Parent set) at its name, replacing what is there. A
// directory over a directory keeps the children unless opaque.
func (b *builder) put(e squashfs.Entry, layer uint8, opaque bool) int32 {
	old := b.lookup(e.Parent, e.Name)
	if old >= 0 {
		oe := &b.entries[old]
		switch {
		case oe.Kind == squashfs.KindDir && e.Kind == squashfs.KindDir:
			*oe = e
			if opaque {
				b.cutoff[old] = int32(len(b.entries))
			}
			return old
		case oe.Kind != squashfs.KindDir && e.Kind != squashfs.KindDir:
			*oe = e
			b.layer[old] = layer
			delete(b.inline, old)
			b.replaced++
			return old
		}
		b.kill(old)
	}
	return b.add(e, layer)
}

// overlay applies one higher layer, in index order so parents come first.
func (b *builder) overlay(fs *squashfs.FS, ix *squashfs.Index, layer uint8) error {
	markers := map[int32]bool{}
	for i := 1; i < len(ix.Entries); i++ {
		if ix.Entries[i].Name == opaqueMarker {
			markers[ix.Entries[i].Parent] = true
		}
	}
	r := &ix.Entries[0]
	root := &b.entries[0]
	root.Perm, root.UID, root.GID, root.Mtime = r.Perm, r.UID, r.GID, r.Mtime
	opaqueRoot, err := isOpaque(fs, r)
	if err != nil {
		return fmt.Errorf("/: %w", err)
	}
	if opaqueRoot || markers[0] {
		b.cutoff[0] = int32(len(b.entries))
		b.st.Opaque++
	}
	at := make([]int32, len(ix.Entries)) // layer index -> merged index of directories
	for i := 1; i < len(ix.Entries); i++ {
		e := ix.Entries[i]
		p := at[e.Parent]
		e.Parent = p
		if e.Name == opaqueMarker {
			continue
		}
		if isWhiteout(&e) {
			b.st.Whiteouts++
			if old := b.lookup(p, whiteoutTarget(&e)); old >= 0 {
				b.kill(old)
			}
			continue
		}
		opaque := markers[int32(i)]
		if e.Kind == squashfs.KindDir && !opaque {
			var err error
			if opaque, err = isOpaque(fs, &e); err != nil {
				return fmt.Errorf("/%s: %w", ix.Path(i), err)
			} else if opaque {
				e.Xattrs--
			}
		}
		if opaque {
			b.st.Opaque++
		}
		at[i] = b.put(e, layer, opaque)
	}
	return nil
}

// merge stacks squashfs layers, lowest first, applying whiteouts and opaque
// directories the way overlayfs presents them. The indexes must be built
// with metadata still cached or readable, since opaque xattrs are read here.
func merge(layers []*squashfs.FS, ixs []*squashfs.Index) (*builder, error) {
	if len(layers) == 0 || len(layers) > maxLayers || len(layers) != len(ixs) {
		return nil, fmt.Errorf("cannot merge %d layers", len(layers))
	}
	b := newBuilder(ixs[0])
	b.st.Layers = len(layers)
	// Whiteouts in the lowest layer hide nothing.
	for i := 1; i < len(b.entries); i++ {
		e := &b.entries[i]
		if isWhiteout(e) || e.Name == opaqueMarker {
			b.dead[i] = true
			b.initialDead++
			b.st.Whiteouts++
		}
	}
	for l := 1; l < len(layers); l++ {
		if err := b.overlay(layers[l], ixs[l], uint8(l)); err != nil {
			return nil, fmt.Errorf("layer %d: %w", l, err)
		}
	}
	return b, nil
}

// addFiles inserts generated files, e.g. the Singularity environment.
func (b *builder) addFiles(files []singularity.File, mtime uint32) error {
	for _, f := range files {
		parent := int32(0)
		parts := strings.Split(f.Path, "/")
		for _, c := range parts[:len(parts)-1] {
			if parent = b.lookup(parent, c); parent < 0 || b.entries[parent].Kind != squashfs.KindDir {
				return fmt.Errorf("generating /%s: parent /%s is not a directory", f.Path, c)
			}
		}
		name := parts[len(parts)-1]
		if f.IfMissing && b.lookup(parent, name) >= 0 {
			continue
		}
		e := squashfs.Entry{Parent: parent, Name: name, Perm: uint16(f.Mode.Perm()), Mtime: mtime, Nlink: 1}
		switch {
		case f.Mode.IsDir():
			e.Kind = squashfs.KindDir
		case f.Mode&os.ModeSymlink != 0:
			e.Kind, e.Target, e.Size = squashfs.KindSymlink, f.Content, uint64(len(f.Content))
		default:
			e.Kind, e.Size, e.Frag = squashfs.KindFile, uint64(len(f.Content)), squashfs.NoFragment
		}
		i := b.put(e, 0, false)
		if e.Kind == squashfs.KindFile {
			b.inline[i] = []byte(f.Content)
		}
		b.st.Generated++
	}
	return nil
}

// build drops the removed entries and returns the merged tree.
func (b *builder) build(layers []*squashfs.FS) (*Tree, MergeStats, error) {
	n := len(b.entries)
	alive := make([]bool, n)
	alive[0] = true
	remap := make([]int32, n)
	ix := squashfs.NewIndex(b.entries[0])
	t := &Tree{Index: ix, Layers: layers, inline: map[int32][]byte{}}
	if len(layers) > 1 {
		t.layer = []uint8{b.layer[0]}
	}
	for i := 1; i < n; i++ {
		e := b.entries[i]
		p := e.Parent
		cut, hasCut := b.cutoff[p]
		if b.dead[i] || !alive[p] || (hasCut && int32(i) < cut) {
			continue
		}
		alive[i] = true
		e.Parent = remap[p]
		j, err := ix.Add(e)
		if err != nil {
			return nil, b.st, err
		}
		remap[i] = j
		if t.layer != nil {
			t.layer = append(t.layer, b.layer[i])
		}
		if d, ok := b.inline[int32(i)]; ok {
			t.inline[j] = d
		}
	}
	b.st.Hidden = n - len(ix.Entries) - b.initialDead + b.replaced
	return t, b.st, nil
}
