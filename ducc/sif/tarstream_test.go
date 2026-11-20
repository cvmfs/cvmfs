package sif

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/google/uuid"

	"github.com/cvmfs/ducc/sif/squashfs"
)

type node struct {
	typ   byte
	mode  int64
	size  int64
	sum   string
	link  string
	mtime int64
	major int64
	minor int64
	inode uint64 // source tree only, to group hardlinks
}

func requireMksquashfs(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("mksquashfs"); err != nil {
		t.Skip("mksquashfs not installed")
	}
}

func writeFile(t *testing.T, path string, data []byte, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

func pattern(n int, seed byte) []byte {
	b := make([]byte, n)
	x := uint32(seed) + 1
	for i := range b {
		x = x*1664525 + 1013904223
		b[i] = byte(x >> 24)
		if i%7 == 0 {
			b[i] = 'a' // keep it compressible
		}
	}
	return b
}

// buildTree creates a tree covering the cases the converter must handle.
func buildTree(t *testing.T, root string) {
	t.Helper()
	writeFile(t, filepath.Join(root, "empty"), nil, 0644)
	writeFile(t, filepath.Join(root, "small"), []byte("hello\n"), 0600)
	writeFile(t, filepath.Join(root, "exactly-one-block"), pattern(128*1024, 1), 0644)
	writeFile(t, filepath.Join(root, "big"), pattern(1000*1000+17, 2), 0755)
	writeFile(t, filepath.Join(root, "setuid"), []byte("#!/bin/sh\n"), 0755|os.ModeSetuid)
	writeFile(t, filepath.Join(root, "a/b/c/d/deep"), []byte("deep"), 0644)
	writeFile(t, filepath.Join(root, "dup1"), pattern(300*1000, 3), 0644)
	writeFile(t, filepath.Join(root, "dup2"), pattern(300*1000, 3), 0644)
	long := strings.Repeat("n", 200)
	writeFile(t, filepath.Join(root, "long", long, long), []byte("long"), 0644)
	writeFile(t, filepath.Join(root, "name with spaces"), []byte("sp"), 0644)

	// sparse file with a hole in the middle block
	sp, err := os.Create(filepath.Join(root, "sparse"))
	if err != nil {
		t.Fatal(err)
	}
	sp.Write(pattern(4096, 4))
	sp.Seek(5*128*1024, io.SeekStart)
	sp.Write([]byte("end"))
	sp.Close()

	// more than 256 entries forces several directory headers
	for i := 0; i < 300; i++ {
		writeFile(t, filepath.Join(root, "many", fmt.Sprintf("f%03d", i)), []byte(fmt.Sprint(i)), 0644)
	}

	if err := os.Link(filepath.Join(root, "big"), filepath.Join(root, "a/big-link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(root, "small"), filepath.Join(root, "small-link")); err != nil {
		t.Fatal(err)
	}
	os.Symlink("a/b/c", filepath.Join(root, "sym"))
	os.Symlink("/etc/passwd", filepath.Join(root, "abs-sym"))
	os.Symlink("sym", filepath.Join(root, "sym-hardlinked"))
	os.Link(filepath.Join(root, "sym-hardlinked"), filepath.Join(root, "a/sym-hardlink2"))
	if err := syscall.Mkfifo(filepath.Join(root, "fifo"), 0644); err != nil {
		t.Fatal(err)
	}
	os.MkdirAll(filepath.Join(root, "emptydir"), 0700)
	os.Chmod(filepath.Join(root, "emptydir"), 0700)
}

func scanTree(t *testing.T, root string) map[string]node {
	t.Helper()
	out := map[string]node{}
	err := filepath.Walk(root, func(p string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		if rel == "." {
			return nil
		}
		st := fi.Sys().(*syscall.Stat_t)
		n := node{mode: int64(fi.Mode().Perm()), mtime: fi.ModTime().Unix(), inode: st.Ino}
		if fi.Mode()&os.ModeSetuid != 0 {
			n.mode |= 04000
		}
		switch {
		case fi.IsDir():
			n.typ = tar.TypeDir
		case fi.Mode()&os.ModeSymlink != 0:
			n.typ = tar.TypeSymlink
			n.link, _ = os.Readlink(p)
		case fi.Mode()&os.ModeNamedPipe != 0:
			n.typ = tar.TypeFifo
		default:
			n.typ = tar.TypeReg
			data, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			n.size = int64(len(data))
			s := sha256.Sum256(data)
			n.sum = hex.EncodeToString(s[:])
		}
		out[rel] = n
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// readTar resolves hardlinks to their targets so the result is comparable
// with scanTree; links records which names were emitted as tar hardlinks.
func readTar(t *testing.T, data []byte) (map[string]node, map[string]string) {
	t.Helper()
	out := map[string]node{}
	links := map[string]string{}
	tr := tar.NewReader(bytes.NewReader(data))
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		name := strings.TrimSuffix(h.Name, "/")
		if _, dup := out[name]; dup {
			t.Fatalf("entry %q emitted twice", name)
		}
		if h.Typeflag == tar.TypeLink {
			target, ok := out[h.Linkname]
			if !ok {
				t.Fatalf("hardlink %q to %q precedes its target", name, h.Linkname)
			}
			links[name] = h.Linkname
			out[name] = target
			continue
		}
		parent := filepath.Dir(name)
		if parent != "." {
			if p, ok := out[parent]; !ok || p.typ != tar.TypeDir {
				t.Fatalf("%q emitted before its parent directory", name)
			}
		}
		n := node{typ: h.Typeflag, mode: h.Mode, size: h.Size, link: h.Linkname,
			mtime: h.ModTime.Unix(), major: h.Devmajor, minor: h.Devminor}
		if h.Typeflag == tar.TypeReg {
			b, err := io.ReadAll(tr)
			if err != nil {
				t.Fatal(err)
			}
			s := sha256.Sum256(b)
			n.sum = hex.EncodeToString(s[:])
			n.size = int64(len(b))
		}
		out[name] = n
	}
	return out, links
}

func mksquashfs(t testing.TB, src, img string, args ...string) {
	t.Helper()
	cmd := exec.Command("mksquashfs", append([]string{src, img, "-noappend", "-no-progress", "-no-xattrs"}, args...)...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("mksquashfs %v: %v\n%s", args, err, out)
	}
}

func convert(t *testing.T, img string, opts StreamOptions) ([]byte, StreamStats, *squashfs.Index) {
	t.Helper()
	f, err := os.Open(img)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fi, _ := f.Stat()
	im, err := Parse(f, fi.Size())
	if err != nil {
		t.Fatal(err)
	}
	fs, err := squashfs.Open(im.Partition(f), im.PartSize)
	if err != nil {
		t.Fatal(err)
	}
	ix, err := fs.BuildIndex(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	fs.DropMetadataCache()
	var buf bytes.Buffer
	st, err := Stream(context.Background(), &buf, fs, ix, opts)
	if err != nil {
		t.Fatal(err)
	}
	return buf.Bytes(), st, ix
}

func compareTrees(t *testing.T, want, got map[string]node) {
	t.Helper()
	for name, w := range want {
		g, ok := got[name]
		if !ok {
			t.Errorf("missing %q", name)
			continue
		}
		w.inode = 0
		if w != g {
			t.Errorf("%q: want %+v, got %+v", name, w, g)
		}
	}
	for name := range got {
		if _, ok := want[name]; !ok && filepath.Base(name) != catalogMarker {
			t.Errorf("unexpected %q", name)
		}
	}
}

func TestStreamMatchesSource(t *testing.T) {
	requireMksquashfs(t)
	src := filepath.Join(t.TempDir(), "src")
	buildTree(t, src)
	want := scanTree(t, src)

	cases := map[string][]string{
		"gzip":             {"-comp", "gzip"},
		"xz":               {"-comp", "xz"},
		"lz4":              {"-comp", "lz4"},
		"zstd":             {"-comp", "zstd"},
		"lzma":             {"-comp", "lzma"},
		"block4k":          {"-b", "4096"},
		"block1m":          {"-b", "1M"},
		"no-fragments":     {"-no-fragments"},
		"always-fragments": {"-always-use-fragments"},
		"no-duplicates":    {"-no-duplicates"},
		"uncompressed":     {"-noI", "-noD", "-noF"},
	}
	for name, args := range cases {
		name, args := name, args
		t.Run(name, func(t *testing.T) {
			img := filepath.Join(t.TempDir(), "img.sqfs")
			cmd := exec.Command("mksquashfs", append([]string{src, img, "-noappend", "-no-progress", "-no-xattrs"}, args...)...)
			if out, err := cmd.CombinedOutput(); err != nil {
				if strings.Contains(string(out), "not supported") || strings.Contains(string(out), "Unrecognised") {
					t.Skipf("mksquashfs lacks %v: %s", args, out)
				}
				t.Fatalf("mksquashfs: %v\n%s", err, out)
			}
			opts := DefaultStreamOptions()
			opts.Workers = 3
			data, st, _ := convert(t, img, opts)
			got, links := readTar(t, data)
			compareTrees(t, want, got)
			// mksquashfs does not consistently keep hardlinked symlinks, so
			// only the regular-file links are required.
			if links["a/big-link"] != "big" || links["small-link"] != "small" || st.Hardlinks != len(links) {
				t.Errorf("hardlinks: stats %d, tar %v", st.Hardlinks, links)
			}
		})
	}
}

func TestStreamDevicesAndOwnership(t *testing.T) {
	requireMksquashfs(t)
	src := filepath.Join(t.TempDir(), "src")
	writeFile(t, filepath.Join(src, "f"), []byte("x"), 0644)
	img := filepath.Join(t.TempDir(), "img.sqfs")
	mksquashfs(t, src, img,
		"-p", "dev d 755 0 0",
		"-p", "dev/null c 666 0 0 1 3",
		"-p", "dev/big c 600 0 0 300 70000",
		"-p", "dev/sda b 660 0 6 8 0",
		"-p", "owned f 640 1234 5678 echo owned",
		"-p", "sock i 777 0 0 s")
	data, st, _ := convert(t, img, DefaultStreamOptions())
	got, _ := readTar(t, data)
	check := func(name string, typ byte, major, minor int64) {
		n, ok := got[name]
		if !ok || n.typ != typ || n.major != major || n.minor != minor {
			t.Errorf("%s: got %+v (present %v)", name, n, ok)
		}
	}
	check("dev/null", tar.TypeChar, 1, 3)
	check("dev/big", tar.TypeChar, 300, 70000)
	check("dev/sda", tar.TypeBlock, 8, 0)
	if _, ok := got["sock"]; ok || st.SocketsDropped != 1 {
		t.Errorf("socket not dropped: stats %d", st.SocketsDropped)
	}

	tr := tar.NewReader(bytes.NewReader(data))
	for {
		h, err := tr.Next()
		if err != nil {
			break
		}
		switch h.Name {
		case "owned":
			if h.Uid != 1234 || h.Gid != 5678 {
				t.Errorf("owned: uid/gid %d/%d", h.Uid, h.Gid)
			}
		case "dev/sda":
			if h.Uid != 0 || h.Gid != 6 {
				t.Errorf("dev/sda: uid/gid %d/%d", h.Uid, h.Gid)
			}
		}
	}
}

func TestStreamOrderIsByDataPosition(t *testing.T) {
	requireMksquashfs(t)
	src := filepath.Join(t.TempDir(), "src")
	buildTree(t, src)
	img := filepath.Join(t.TempDir(), "img.sqfs")
	mksquashfs(t, src, img)

	f, _ := os.Open(img)
	defer f.Close()
	fi, _ := f.Stat()
	fs, err := squashfs.Open(f, fi.Size())
	if err != nil {
		t.Fatal(err)
	}
	ix, err := fs.BuildIndex(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	order, _ := Order(SingleTree(fs, ix), nil, nil)
	if len(order) != len(ix.Entries)-1 {
		t.Fatalf("order has %d entries, index %d", len(order), len(ix.Entries))
	}
	seen := map[int32]bool{}
	phase := 0
	var last uint64
	for _, i := range order {
		if seen[i] {
			t.Fatalf("entry %d twice", i)
		}
		seen[i] = true
		e := &ix.Entries[i]
		p := 2
		if e.Kind == squashfs.KindDir {
			p = 0
		} else if e.Kind != squashfs.KindFile || e.Size == 0 {
			p = 1
		}
		if p < phase {
			t.Fatalf("%s out of phase order", ix.Path(int(i)))
		}
		phase = p
		if p == 2 {
			pos := e.BlocksStart
			if len(e.Blocks) == 0 {
				pos, _, _ = fs.Fragment(e.Frag)
			}
			if pos < last {
				t.Fatalf("%s at %d before %d", ix.Path(int(i)), pos, last)
			}
			last = pos
		}
	}
}

func TestPlaceCatalogs(t *testing.T) {
	requireMksquashfs(t)
	src := filepath.Join(t.TempDir(), "src")
	for d := 0; d < 4; d++ {
		for i := 0; i < 50; i++ {
			writeFile(t, filepath.Join(src, fmt.Sprintf("top/d%d/f%d", d, i)), nil, 0644)
		}
	}
	writeFile(t, filepath.Join(src, "tiny/f"), nil, 0644)
	img := filepath.Join(t.TempDir(), "img.sqfs")
	mksquashfs(t, src, img)

	opts := DefaultStreamOptions()
	opts.CatalogMaxWeight, opts.CatalogMinWeight = 120, 10
	data, st, _ := convert(t, img, opts)
	got, _ := readTar(t, data)
	var markers []string
	for name := range got {
		if filepath.Base(name) == catalogMarker {
			markers = append(markers, filepath.Dir(name))
		}
	}
	// top has 205 entries: two of its 51-entry children are split off to
	// bring it under 120; tiny is below the minimum weight.
	if len(markers) != 2 || st.Catalogs != 2 {
		t.Fatalf("markers %v, stats %d", markers, st.Catalogs)
	}
	for _, m := range markers {
		if !strings.HasPrefix(m, "top/d") {
			t.Errorf("unexpected marker in %s", m)
		}
	}
}

func TestExistingCatalogMarker(t *testing.T) {
	requireMksquashfs(t)
	src := filepath.Join(t.TempDir(), "src")
	writeFile(t, filepath.Join(src, "a/.cvmfscatalog"), nil, 0644)
	os.MkdirAll(filepath.Join(src, "b"), 0755)
	os.Symlink("x", filepath.Join(src, "b/.cvmfscatalog"))
	img := filepath.Join(t.TempDir(), "img.sqfs")
	mksquashfs(t, src, img)
	data, st, _ := convert(t, img, DefaultStreamOptions())
	got, _ := readTar(t, data)
	if _, ok := got["b/.cvmfscatalog"]; ok || st.MarkersDropped != 1 {
		t.Errorf("symlink marker not dropped (stats %d)", st.MarkersDropped)
	}
	if n, ok := got["a/.cvmfscatalog"]; !ok || n.typ != tar.TypeReg {
		t.Errorf("regular marker missing")
	}
}

// writeSIF wraps a squashfs image into a minimal SIF container.
func writeSIF(t testing.TB, sqfs, out string, fstype, parttype int32, extra ...int32) {
	t.Helper()
	part, err := os.ReadFile(sqfs)
	if err != nil {
		t.Fatal(err)
	}
	const ndesc = 4
	descOff := int64(4096)
	dataOff := descOff + int64(ndesc*descriptorSize)
	dataOff = (dataOff + 4095) &^ 4095
	h := rawHeader{
		Magic: sifMagic, Version: [3]byte{'0', '1', 0}, ID: uuid.New(),
		CreatedAt: 1700000000, DescriptorsTotal: ndesc, DescriptorsFree: ndesc - 1 - int64(len(extra)),
		DescriptorsOffset: descOff, DescriptorsSize: ndesc * int64(descriptorSize),
		DataOffset: dataOff, DataSize: int64(len(part)),
	}
	copy(h.LaunchScript[:], "#!/usr/bin/env run-singularity\n")
	descs := make([]rawDescriptor, ndesc)
	descs[0] = rawDescriptor{DataType: DataPartition, Used: true, ID: 1, Offset: dataOff, Size: int64(len(part))}
	binary.LittleEndian.PutUint32(descs[0].Extra[0:], uint32(fstype))
	binary.LittleEndian.PutUint32(descs[0].Extra[4:], uint32(parttype))
	copy(descs[0].Extra[8:], "02\x00")
	for i, typ := range extra {
		descs[i+1] = rawDescriptor{DataType: typ, Used: true, ID: uint32(i + 2), Offset: 0, Size: 0}
	}
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, h)
	buf.Write(make([]byte, int(descOff)-buf.Len()))
	binary.Write(&buf, binary.LittleEndian, descs)
	buf.Write(make([]byte, int(dataOff)-buf.Len()))
	buf.Write(part)
	if err := os.WriteFile(out, buf.Bytes(), 0644); err != nil {
		t.Fatal(err)
	}
}

func TestParseSIF(t *testing.T) {
	requireMksquashfs(t)
	src := filepath.Join(t.TempDir(), "src")
	buildTree(t, src)
	dir := t.TempDir()
	img := filepath.Join(dir, "img.sqfs")
	mksquashfs(t, src, img)

	sifPath := filepath.Join(dir, "img.sif")
	writeSIF(t, img, sifPath, FsSquash, PartPrimSys, DataSignature, DataDeffile)
	data, _, _ := convert(t, sifPath, DefaultStreamOptions())
	got, _ := readTar(t, data)
	compareTrees(t, scanTree(t, src), got)

	f, _ := os.Open(sifPath)
	fi, _ := f.Stat()
	im, err := Parse(f, fi.Size())
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	if im.Bare || im.Arch != "amd64" || !im.Signed || len(im.HeaderDigest()) != 64 || len(im.Descs) != 3 {
		t.Errorf("unexpected image metadata %+v", im)
	}

	bad := map[string]func(){
		"ext3":       func() { writeSIF(t, img, sifPath, FsExt3, PartPrimSys) },
		"encrypted":  func() { writeSIF(t, img, sifPath, FsEncryptedSquashfs, PartPrimSys) },
		"no primary": func() { writeSIF(t, img, sifPath, FsSquash, PartData) },
		"oci":        func() { writeSIF(t, img, sifPath, FsSquash, PartData, DataOCIRootIndex, DataOCIRootIndex) },
	}
	wantErr := map[string]string{
		"ext3": "ext3", "encrypted": "encrypted", "no primary": "no primary", "oci": "more than one root index",
	}
	for name, mk := range bad {
		mk()
		f, _ := os.Open(sifPath)
		fi, _ := f.Stat()
		im, err := Parse(f, fi.Size())
		if err == nil && im.OCI {
			_, err = im.ReadOCI(f)
		}
		f.Close()
		if err == nil || !strings.Contains(err.Error(), wantErr[name]) {
			t.Errorf("%s: got error %v", name, err)
		}
	}
}

func TestCorruptImagesDoNotPanic(t *testing.T) {
	requireMksquashfs(t)
	src := filepath.Join(t.TempDir(), "src")
	buildTree(t, src)
	img := filepath.Join(t.TempDir(), "img.sqfs")
	mksquashfs(t, src, img)
	orig, _ := os.ReadFile(img)

	x := uint32(12345)
	for round := 0; round < 300; round++ {
		data := append([]byte(nil), orig...)
		for k := 0; k < 8; k++ {
			x = x*1664525 + 1013904223
			pos := int(x>>8) % len(data)
			if round%3 == 0 {
				pos = len(data) - 1 - int(x>>8)%4096 // metadata tables live at the end
			}
			if pos < 0 {
				pos = 0
			}
			data[pos] ^= byte(x)
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("round %d: panic %v", round, r)
				}
			}()
			r := bytes.NewReader(data)
			fs, err := squashfs.Open(r, int64(len(data)))
			if err != nil {
				return
			}
			ix, err := fs.BuildIndex(1 << 16)
			if err != nil {
				return
			}
			Stream(context.Background(), io.Discard, fs, ix, DefaultStreamOptions())
		}()
	}
}

// checkContiguous verifies the --contiguous-catalogs contract on a tar
// stream: after the directories, every catalog's entries form one run that
// starts with its marker, and hardlinks only point into open catalogs.
func checkContiguous(t *testing.T, data []byte, checkLinks bool) {
	t.Helper()
	under := func(p, dir string) bool { return dir == "" || p == dir || strings.HasPrefix(p, dir+"/") }
	tr := tar.NewReader(bytes.NewReader(data))
	open := []string{""}
	closed := map[string]bool{}
	catOf := map[string]string{}
	dirsDone := false
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		name := strings.TrimSuffix(h.Name, "/")
		if h.Typeflag == tar.TypeDir {
			if dirsDone {
				t.Fatalf("directory %s after the directory phase", name)
			}
			continue
		}
		dirsDone = true
		for len(open) > 1 && !under(name, open[len(open)-1]) {
			closed[open[len(open)-1]] = true
			open = open[:len(open)-1]
		}
		for c := range closed {
			if under(name, c) {
				t.Fatalf("%s comes after its catalog %s was left", name, c)
			}
		}
		if filepath.Base(name) == catalogMarker {
			dir := filepath.Dir(name)
			if dir != "." {
				open = append(open, dir)
			}
			continue
		}
		catOf[name] = open[len(open)-1]
		if checkLinks && h.Typeflag == tar.TypeLink {
			tc := catOf[h.Linkname]
			ok := false
			for _, o := range open {
				ok = ok || o == tc
			}
			if !ok {
				t.Fatalf("hardlink %s -> %s points into closed catalog %q", name, h.Linkname, tc)
			}
		}
	}
}

func TestContiguousCatalogs(t *testing.T) {
	requireMksquashfs(t)
	src := filepath.Join(t.TempDir(), "src")
	for i := 0; i < 60; i++ {
		writeFile(t, filepath.Join(src, fmt.Sprintf("top/d0/f%d", i)), pattern(3000+i, byte(i)), 0644)
	}
	for i := 0; i < 55; i++ {
		writeFile(t, filepath.Join(src, fmt.Sprintf("top/d1/sub/f%d", i)), pattern(200000+i, byte(i)), 0644)
	}
	for i := 0; i < 5; i++ {
		writeFile(t, filepath.Join(src, fmt.Sprintf("top/d2/f%d", i)), []byte("x"), 0644)
	}
	writeFile(t, filepath.Join(src, ".cvmfscatalog"), nil, 0644)
	writeFile(t, filepath.Join(src, "top/x"), pattern(5000, 9), 0644)
	// sibling catalogs: must not stay a hardlink in contiguous mode
	os.Link(filepath.Join(src, "top/d0/f0"), filepath.Join(src, "top/d1/sub/link-to-d0"))
	// into the enclosing base catalog: may stay a hardlink
	os.Link(filepath.Join(src, "top/x"), filepath.Join(src, "top/d0/link-to-x"))
	want := scanTree(t, src)
	img := filepath.Join(t.TempDir(), "img.sqfs")
	mksquashfs(t, src, img)

	for _, contiguous := range []bool{false, true} {
		opts := DefaultStreamOptions()
		opts.CatalogMaxWeight, opts.CatalogMinWeight = 50, 10
		opts.ContiguousCatalogs = contiguous
		data, st, _ := convert(t, img, opts)
		got, links := readTar(t, data)
		compareTrees(t, want, got)
		if st.Catalogs != 2 {
			t.Errorf("contiguous=%v: %d catalogs, want 2 (d0, d1)", contiguous, st.Catalogs)
		}
		if _, ok := got[".cvmfscatalog"]; !ok {
			t.Errorf("root .cvmfscatalog of the image lost")
		}
		checkContiguous(t, data, contiguous)
		_, crossLinked := links["top/d1/sub/link-to-d0"]
		_, upLinked := links["top/d0/link-to-x"]
		if !upLinked && links["top/x"] == "" {
			t.Errorf("contiguous=%v: link into enclosing catalog was not kept: %v", contiguous, links)
		}
		if contiguous && crossLinked {
			t.Errorf("sibling-catalog hardlink kept in contiguous mode")
		}
		if !contiguous && !crossLinked && links["top/d0/f0"] == "" {
			t.Errorf("hardlink dropped without contiguous mode: %v", links)
		}
	}
}
