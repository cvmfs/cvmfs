package sif

import (
	"archive/tar"
	"container/list"
	"context"
	"fmt"
	"io"
	"runtime"
	"sort"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/cvmfs/ducc/sif/squashfs"
)

const catalogMarker = ".cvmfscatalog"

type StreamOptions struct {
	// Nested catalog placement by subtree entry count; 0 disables. Defaults
	// match cvmfs autocatalogs (swissknife_sync.h kDefaultMaxWeight/MinWeight).
	CatalogMaxWeight int
	CatalogMinWeight int
	// Workers decompressing data blocks ahead of the tar writer.
	Workers int
	// WindowBytes bounds decompressed data held ahead of the writer.
	WindowBytes int
	// FragmentCache is the number of decompressed fragment blocks kept.
	FragmentCache int
	// ContiguousCatalogs restricts hardlinks to the current or an enclosing
	// catalog, as required by `cvmfs_server ingest --contiguous-catalogs`.
	ContiguousCatalogs bool
	Logger             *log.Entry
}

func DefaultStreamOptions() StreamOptions {
	return StreamOptions{
		CatalogMaxWeight: 100000,
		CatalogMinWeight: 1000,
		Workers:          runtime.NumCPU(),
		WindowBytes:      64 << 20,
		FragmentCache:    64,
	}
}

type StreamStats struct {
	Entries        int   `json:"entries"`
	Directories    int   `json:"directories"`
	Files          int   `json:"files"`
	Hardlinks      int   `json:"hardlinks"`
	Bytes          int64 `json:"bytes"`
	Catalogs       int   `json:"catalogs"`
	XattrInodes    int   `json:"xattr_inodes"`
	XattrsDropped  int   `json:"xattrs"`
	SocketsDropped int   `json:"sockets"`
	// Non-regular entries named .cvmfscatalog; the tarball engine aborts on
	// those, so they are dropped.
	MarkersDropped int           `json:"bad_catalog_markers"`
	Elapsed        time.Duration `json:"-"`
}

// Order returns the emission order and each entry's catalog (the index of
// the directory rooting it; 0 for the ingest base catalog). Directories
// come first, parents before children. Then, depth first over the catalog
// tree, each nested catalog's marker followed by its entries: entries
// without data, then files by on-disk data position so the partition is
// read nearly front to back. Keeping every catalog contiguous lets the
// tarball engine finish and upload it as soon as the stream leaves it.
//
// A negative element -(d+1) stands for a synthetic marker in directory d;
// ownMarker maps directories to the image's own regular .cvmfscatalog file,
// which is emitted in its place.
func Order(fs *squashfs.FS, ix *squashfs.Index, catalogs map[int32]bool, ownMarker map[int32]int32) ([]int32, []int32) {
	n := len(ix.Entries)
	catOf := make([]int32, n)
	members := map[int32][]int32{}
	children := map[int32][]int32{}
	var dirs []int32
	isOwnMarker := map[int32]bool{}
	for _, m := range ownMarker {
		isOwnMarker[m] = true
	}
	for i := 1; i < n; i++ {
		e := &ix.Entries[i]
		catOf[i] = catOf[e.Parent]
		if e.Kind == squashfs.KindDir {
			dirs = append(dirs, int32(i))
			if catalogs[int32(i)] {
				catOf[i] = int32(i)
				children[catOf[e.Parent]] = append(children[catOf[e.Parent]], int32(i))
			}
			continue
		}
		if !isOwnMarker[int32(i)] {
			members[catOf[i]] = append(members[catOf[i]], int32(i))
		}
	}

	key := func(i int32) (uint64, uint32) {
		e := &ix.Entries[i]
		if len(e.Blocks) > 0 {
			return e.BlocksStart, 0
		}
		pos, _, _ := fs.Fragment(e.Frag)
		return pos, e.FragOff
	}
	hasData := func(i int32) bool {
		e := &ix.Entries[i]
		return e.Kind == squashfs.KindFile && e.Size > 0
	}

	out := make([]int32, 0, n+len(catalogs))
	out = append(out, dirs...)
	var emit func(cat int32)
	emit = func(cat int32) {
		if cat != 0 {
			if m, ok := ownMarker[cat]; ok {
				out = append(out, m)
			} else {
				out = append(out, -(cat + 1))
			}
		}
		group := members[cat]
		sort.SliceStable(group, func(a, b int) bool {
			da, db := hasData(group[a]), hasData(group[b])
			if da != db || !da {
				return !da && db
			}
			pa, oa := key(group[a])
			pb, ob := key(group[b])
			if pa != pb {
				return pa < pb
			}
			return oa < ob
		})
		out = append(out, group...)
		for _, c := range children[cat] {
			emit(c)
		}
	}
	emit(0)
	return out, catOf
}

// PlaceCatalogs picks directories that get a nested catalog marker. Bottom
// up, when a directory's remaining subtree weight reaches max, its heaviest
// child directories (each at least min) are split off until it fits; a
// directory that still does not fit is split off itself. The root is never
// returned: ingest --catalog covers it.
func PlaceCatalogs(ix *squashfs.Index, max, min int) map[int32]bool {
	marks := map[int32]bool{}
	if max <= 0 {
		return marks
	}
	n := len(ix.Entries)
	weight := make([]int, n)
	children := make([][]int32, n)
	for i := n - 1; i >= 0; i-- {
		weight[i]++
		e := &ix.Entries[i]
		if e.Kind == squashfs.KindDir {
			// Children have larger indices, so they are final here.
			ch := children[i]
			sort.Slice(ch, func(a, b int) bool { return weight[ch[a]] > weight[ch[b]] })
			for _, c := range ch {
				if weight[i] < max || weight[c] < min {
					break
				}
				marks[c] = true
				weight[i] -= weight[c]
			}
			children[i] = nil
			if i != 0 && weight[i] >= max {
				marks[int32(i)] = true
				weight[i] = 0
			}
		}
		if e.Parent >= 0 {
			weight[e.Parent] += weight[i]
			if e.Kind == squashfs.KindDir {
				children[e.Parent] = append(children[e.Parent], int32(i))
			}
		}
	}
	return marks
}

type blockJob struct {
	pos    uint64
	word   uint32
	want   int
	frag   bool
	fragID uint32
	off    int
	res    chan blockResult
}

type blockResult struct {
	data []byte
	buf  []byte // to return to the pool
	err  error
}

// Stream writes the image as a tar archive to w. Names are relative to the
// image root; the root directory itself is not emitted.
func Stream(ctx context.Context, w io.Writer, fs *squashfs.FS, ix *squashfs.Index, opts StreamOptions) (StreamStats, error) {
	start := time.Now()
	logger := opts.Logger
	if logger == nil {
		logger = log.NewEntry(log.StandardLogger())
	}
	if opts.Workers <= 0 {
		opts.Workers = 1
	}
	bs := fs.BlockSize()
	tokens := opts.WindowBytes / bs
	if tokens < 2*opts.Workers {
		tokens = 2 * opts.Workers
	}

	catalogs := PlaceCatalogs(ix, opts.CatalogMaxWeight, opts.CatalogMinWeight)
	var st StreamStats

	ownMarker := map[int32]int32{}
	skip := make(map[int32]bool)
	for i := range ix.Entries {
		e := &ix.Entries[i]
		if e.Name != catalogMarker {
			continue
		}
		if e.Kind == squashfs.KindFile {
			// At the root it is an ordinary file: ingest --catalog already
			// makes the base directory a catalog.
			if e.Parent != 0 {
				ownMarker[e.Parent] = int32(i)
				catalogs[e.Parent] = true
			}
		} else {
			skip[int32(i)] = true
			st.MarkersDropped++
			logger.WithField("path", ix.Path(i)).Warn("dropping non-regular .cvmfscatalog entry")
		}
	}

	order, catOf := Order(fs, ix, catalogs, ownMarker)
	targets := linkTargets(ix, order, catOf, opts.ContiguousCatalogs)
	secondLink := func(i int32) (int32, bool) {
		t, ok := targets[i]
		return t, ok
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	pool := sync.Pool{New: func() interface{} { return make([]byte, bs) }}
	frags := newFragCache(fs, opts.FragmentCache)
	work := make(chan *blockJob, tokens)
	ordered := make(chan *blockJob, tokens)
	sem := make(chan struct{}, tokens)

	var wg sync.WaitGroup
	for i := 0; i < opts.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range work {
				if ctx.Err() != nil {
					j.res <- blockResult{err: ctx.Err()}
					continue
				}
				if j.frag {
					data, err := frags.get(j.fragID)
					if err == nil && j.off+j.want > len(data) {
						err = fmt.Errorf("fragment %d: tail %d+%d beyond %d bytes", j.fragID, j.off, j.want, len(data))
					}
					if err != nil {
						j.res <- blockResult{err: err}
					} else {
						j.res <- blockResult{data: data[j.off : j.off+j.want]}
					}
					continue
				}
				buf := pool.Get().([]byte)
				data, err := fs.ReadBlock(buf[:0], j.pos, j.word, j.want)
				j.res <- blockResult{data: data, buf: buf, err: err}
			}
		}()
	}

	// Producer: dispatches block jobs in emission order, bounded by sem.
	go func() {
		defer close(ordered)
		defer close(work)
		for _, i := range order {
			if i < 0 {
				continue
			}
			e := &ix.Entries[i]
			if e.Kind != squashfs.KindFile || e.Size == 0 || skip[i] {
				continue
			}
			if _, ok := secondLink(i); ok {
				continue
			}
			for _, j := range fileJobs(e, bs) {
				select {
				case sem <- struct{}{}:
				case <-ctx.Done():
					return
				}
				j.res = make(chan blockResult, 1)
				select {
				case work <- j:
				case <-ctx.Done():
					return
				}
				select {
				case ordered <- j:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	err := writeTar(ctx, w, ix, order, skip, secondLink, ordered, sem, &pool, &st)
	cancel()
	for range ordered {
	}
	wg.Wait()
	st.Elapsed = time.Since(start)
	return st, err
}

func isLink(e *squashfs.Entry) bool { return e.Nlink > 1 && e.Kind != squashfs.KindDir }

// linkTargets maps each path that becomes a tar hardlink to the earlier
// path carrying the data. With contiguous catalogs a link may only point
// into its own catalog or an enclosing one, since other catalogs are
// already finalised; otherwise the path carries the data itself (storage
// deduplicates it).
func linkTargets(ix *squashfs.Index, order, catOf []int32, contiguous bool) map[int32]int32 {
	reachable := func(from, to int32) bool {
		for c := from; ; {
			if c == to {
				return true
			}
			if c == 0 {
				return false
			}
			c = catOf[ix.Entries[c].Parent]
		}
	}
	carriers := map[uint32][]int32{}
	targets := map[int32]int32{}
	for _, i := range order {
		if i < 0 {
			continue
		}
		e := &ix.Entries[i]
		if !isLink(e) {
			continue
		}
		linked := false
		for _, c := range carriers[e.Inode] {
			if !contiguous || reachable(catOf[i], catOf[c]) {
				targets[i] = c
				linked = true
				break
			}
		}
		if !linked {
			carriers[e.Inode] = append(carriers[e.Inode], i)
		}
	}
	return targets
}

func fileJobs(e *squashfs.Entry, bs int) []*blockJob {
	jobs := make([]*blockJob, 0, len(e.Blocks)+1)
	pos := e.BlocksStart
	remaining := int64(e.Size)
	for _, w := range e.Blocks {
		want := bs
		if remaining < int64(bs) {
			want = int(remaining)
		}
		jobs = append(jobs, &blockJob{pos: pos, word: w, want: want})
		pos += uint64(w & (1<<24 - 1))
		remaining -= int64(want)
	}
	if tail := e.TailSize(bs); tail > 0 {
		jobs = append(jobs, &blockJob{frag: true, fragID: e.Frag, off: int(e.FragOff), want: tail})
	}
	return jobs
}

func writeTar(ctx context.Context, w io.Writer, ix *squashfs.Index, order []int32,
	skip map[int32]bool, secondLink func(int32) (int32, bool),
	ordered <-chan *blockJob, sem chan struct{}, pool *sync.Pool, st *StreamStats) error {

	tw := tar.NewWriter(w)
	hdr := func(e *squashfs.Entry, name string) *tar.Header {
		return &tar.Header{
			Name:    name,
			Mode:    int64(e.Perm),
			Uid:     int(e.UID),
			Gid:     int(e.GID),
			ModTime: time.Unix(int64(e.Mtime), 0),
		}
	}

	if len(order) > 0 && ix.Entries[0].Xattrs > 0 {
		st.XattrInodes++
		st.XattrsDropped += int(ix.Entries[0].Xattrs)
	}
	for _, i := range order {
		if err := ctx.Err(); err != nil {
			return err
		}
		if i < 0 {
			d := &ix.Entries[-i-1]
			st.Catalogs++
			if err := tw.WriteHeader(&tar.Header{
				Typeflag: tar.TypeReg, Name: ix.Path(int(-i-1)) + "/" + catalogMarker, Mode: 0644,
				Uid: int(d.UID), Gid: int(d.GID), ModTime: time.Unix(int64(d.Mtime), 0),
			}); err != nil {
				return err
			}
			continue
		}
		e := &ix.Entries[i]
		if skip[i] {
			continue
		}
		name := ix.Path(int(i))
		st.Entries++
		if e.Xattrs > 0 {
			// TODO(xattr): the tarball engine does not import xattrs, so they
			// are dropped (security.capability matters most). Once it does,
			// emit PAX SCHILY.xattr.* records here behind a flag.
			st.XattrInodes++
			st.XattrsDropped += int(e.Xattrs)
		}
		h := hdr(e, name)
		switch e.Kind {
		case squashfs.KindDir:
			st.Directories++
			h.Typeflag, h.Name = tar.TypeDir, name+"/"
		case squashfs.KindSymlink:
			h.Typeflag, h.Linkname = tar.TypeSymlink, e.Target
		case squashfs.KindChar, squashfs.KindBlock:
			h.Typeflag = tar.TypeChar
			if e.Kind == squashfs.KindBlock {
				h.Typeflag = tar.TypeBlock
			}
			h.Devmajor, h.Devminor = e.Major(), e.Minor()
		case squashfs.KindFifo:
			h.Typeflag = tar.TypeFifo
		case squashfs.KindSocket:
			st.Entries--
			st.SocketsDropped++
			continue
		case squashfs.KindFile:
			h.Typeflag, h.Size = tar.TypeReg, int64(e.Size)
		}
		if target, ok := secondLink(i); ok {
			h.Typeflag, h.Size = tar.TypeLink, 0
			h.Linkname = ix.Path(int(target))
			st.Hardlinks++
			if err := tw.WriteHeader(h); err != nil {
				return err
			}
			continue
		}
		if err := tw.WriteHeader(h); err != nil {
			return err
		}
		if e.Kind != squashfs.KindFile {
			continue
		}
		st.Files++
		if e.Size == 0 {
			continue
		}
		var written int64
		for written < int64(e.Size) {
			var j *blockJob
			select {
			case j = <-ordered:
			case <-ctx.Done():
				return ctx.Err()
			}
			if j == nil {
				return fmt.Errorf("%s: block pipeline ended early", name)
			}
			r := <-j.res
			if r.err != nil {
				return fmt.Errorf("%s: %w", name, r.err)
			}
			_, err := tw.Write(r.data)
			if r.buf != nil {
				pool.Put(r.buf[:cap(r.buf)])
			}
			<-sem
			if err != nil {
				return err
			}
			written += int64(len(r.data))
		}
		st.Bytes += written
	}
	return tw.Close()
}

// fragCache keeps decompressed fragment blocks; files sharing a fragment
// are adjacent in emission order, so a small LRU suffices.
type fragCache struct {
	fs    *squashfs.FS
	max   int
	mu    sync.Mutex
	lru   *list.List
	items map[uint32]*list.Element
}

type fragItem struct {
	id    uint32
	ready chan struct{}
	data  []byte
	err   error
}

func newFragCache(fs *squashfs.FS, max int) *fragCache {
	if max < 1 {
		max = 1
	}
	return &fragCache{fs: fs, max: max, lru: list.New(), items: map[uint32]*list.Element{}}
}

func (c *fragCache) get(id uint32) ([]byte, error) {
	c.mu.Lock()
	if el, ok := c.items[id]; ok {
		c.lru.MoveToFront(el)
		it := el.Value.(*fragItem)
		c.mu.Unlock()
		<-it.ready
		return it.data, it.err
	}
	it := &fragItem{id: id, ready: make(chan struct{})}
	c.items[id] = c.lru.PushFront(it)
	for c.lru.Len() > c.max {
		old := c.lru.Back()
		c.lru.Remove(old)
		delete(c.items, old.Value.(*fragItem).id)
	}
	c.mu.Unlock()
	it.data, it.err = c.fs.ReadFragment(id)
	close(it.ready)
	if it.err != nil {
		c.mu.Lock()
		if el, ok := c.items[id]; ok && el.Value.(*fragItem) == it {
			c.lru.Remove(el)
			delete(c.items, id)
		}
		c.mu.Unlock()
	}
	return it.data, it.err
}
