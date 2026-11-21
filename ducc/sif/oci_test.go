package sif

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	digest "github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/cvmfs/ducc/singularity"
)

type ociBlob struct {
	typ  int32
	data []byte
}

func sha(b []byte) string {
	s := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(s[:])
}

// ociParts are the pieces of a synthetic OCI-SIF; tests corrupt them
// before writeOCISIF.
type ociParts struct {
	manifestType string
	layers       [][]byte
	types        []string
	config       []byte
	manifest     []byte
	index        []byte
	extra        []ociBlob
}

func newOCIParts(t testing.TB, layerFiles []string, cfg ocispec.Image) *ociParts {
	t.Helper()
	p := &ociParts{manifestType: ocispec.MediaTypeImageManifest}
	for _, f := range layerFiles {
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		p.layers = append(p.layers, data)
		p.types = append(p.types, MediaTypeSquashfsLayer)
	}
	p.config, _ = json.Marshal(cfg)
	p.seal()
	return p
}

// seal recomputes the manifest and index from the layers and config.
func (p *ociParts) seal() {
	m := ocispec.Manifest{
		Versioned: specs.Versioned{SchemaVersion: 2},
		MediaType: p.manifestType,
		Config: ocispec.Descriptor{MediaType: ocispec.MediaTypeImageConfig,
			Digest: digest.Digest(sha(p.config)), Size: int64(len(p.config))},
	}
	for i, l := range p.layers {
		m.Layers = append(m.Layers, ocispec.Descriptor{MediaType: p.types[i],
			Digest: digest.Digest(sha(l)), Size: int64(len(l))})
	}
	p.manifest, _ = json.Marshal(m)
	idx := ocispec.Index{Versioned: specs.Versioned{SchemaVersion: 2},
		Manifests: []ocispec.Descriptor{{MediaType: p.manifestType,
			Digest: digest.Digest(sha(p.manifest)), Size: int64(len(p.manifest))}}}
	p.index, _ = json.Marshal(idx)
}

// writeOCISIF lays the blobs out like SingularityCE does: layers, config,
// manifest, then the root index.
func writeOCISIF(t testing.TB, p *ociParts, out string) {
	t.Helper()
	var objs []sifObject
	for _, l := range p.layers {
		objs = append(objs, sifObject{typ: DataOCIBlob, data: l})
	}
	objs = append(objs, sifObject{typ: DataOCIBlob, data: p.config}, sifObject{typ: DataOCIBlob, data: p.manifest})
	for _, b := range p.extra {
		objs = append(objs, sifObject{typ: b.typ, data: b.data})
	}
	objs = append(objs, sifObject{typ: DataOCIRootIndex, data: p.index})
	createSIF(t, out, objs)
}

// mklayer builds a squashfs layer keeping xattrs, with pseudo definitions
// for whiteout devices and opaque xattrs that need root on a real fs.
func mklayer(t testing.TB, src, img string, pseudo ...string) {
	t.Helper()
	args := []string{src, img, "-noappend", "-no-progress", "-all-root", "-all-time", "1700000000"}
	for _, p := range pseudo {
		args = append(args, "-p", p)
	}
	if out, err := exec.Command("mksquashfs", args...).CombinedOutput(); err != nil {
		t.Fatalf("mksquashfs %v: %v\n%s", pseudo, err, out)
	}
}

func testConfig() ocispec.Image {
	created := time.Unix(1700000000, 0).UTC()
	return ocispec.Image{
		Created:  &created,
		Platform: ocispec.Platform{Architecture: "amd64", OS: "linux"},
		Config: ocispec.ImageConfig{
			Env:        []string{"PATH=/usr/bin:/bin", `FOO=a b$c"d\e`, "EMPTY"},
			Entrypoint: []string{"/bin/sh", "-c"},
			Cmd:        []string{"echo hi"},
		},
	}
}

func streamOCI(t *testing.T, path string) (*Inspected, []byte) {
	t.Helper()
	in, err := Inspect(context.Background(), path, "", 0, DefaultSourceOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer in.Src.Close()
	var buf bytes.Buffer
	opts := DefaultStreamOptions()
	opts.Workers = 3
	if _, err := StreamTree(context.Background(), &buf, in.Tree, opts); err != nil {
		t.Fatal(err)
	}
	return in, buf.Bytes()
}

// expectedSingularity is what the stream must add to an image's tree.
func expectedSingularity(cfg ocispec.Image, present map[string]node) map[string]node {
	out := map[string]node{}
	for _, f := range singularity.Files(cfg) {
		if _, ok := present[f.Path]; ok && f.IfMissing {
			continue
		}
		n := node{mode: int64(f.Mode.Perm()), mtime: cfg.Created.Unix()}
		switch {
		case f.Mode.IsDir():
			n.typ = tar.TypeDir
		case f.Mode&os.ModeSymlink != 0:
			n.typ, n.link = tar.TypeSymlink, f.Content
		default:
			s := sha256.Sum256([]byte(f.Content))
			n.typ, n.size, n.sum = tar.TypeReg, int64(len(f.Content)), hex.EncodeToString(s[:])
		}
		out[f.Path] = n
	}
	return out
}

func noMtime(m map[string]node) map[string]node {
	for k, n := range m {
		n.mtime = 0
		m[k] = n
	}
	return m
}

func TestOCISIFSingleLayer(t *testing.T) {
	requireMksquashfs(t)
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	buildTree(t, src)
	writeFile(t, filepath.Join(src, "etc/hosts"), []byte("127.0.0.1 localhost\n"), 0644)
	img := filepath.Join(dir, "layer.sqfs")
	mksquashfs(t, src, img)
	cfg := testConfig()
	p := newOCIParts(t, []string{img}, cfg)
	p.manifestType = mediaTypeDockerManifest
	p.seal()
	sifPath := filepath.Join(dir, "oci.sif")
	writeOCISIF(t, p, sifPath)

	in, data := streamOCI(t, sifPath)
	if in.IDKind != "oci-config" || "sha256:"+in.ID != sha(p.config) {
		t.Errorf("ID %s (%s), want config digest %s", in.ID, in.IDKind, sha(p.config))
	}
	if in.Image.Arch != "amd64" || in.OCI.ManifestDigest != sha(p.manifest) || len(in.OCI.Layers) != 1 {
		t.Errorf("unexpected OCI metadata %+v / %+v", in.Image, in.OCI)
	}
	if in.DataSize() != int64(len(p.layers[0])) {
		t.Errorf("data size %d", in.DataSize())
	}
	got, _ := readTar(t, data)
	want := scanTree(t, src)
	for k, v := range expectedSingularity(cfg, want) {
		want[k] = v
	}
	compareTrees(t, want, got)
	checkContiguous(t, data, true)
}

func TestOCISIFLayerMerge(t *testing.T) {
	requireMksquashfs(t)
	dir := t.TempDir()
	l1, l2, l3 := filepath.Join(dir, "l1"), filepath.Join(dir, "l2"), filepath.Join(dir, "l3")
	for _, f := range []string{"a/f1", "a/f2", "a/sub/g", "b/x", "b/keep/k", "top", "c/1", "gone/deep", "d/e/f"} {
		writeFile(t, filepath.Join(l1, f), []byte(f+" v1\n"), 0644)
	}
	writeFile(t, filepath.Join(l1, "hl1"), pattern(300000, 5), 0644)
	os.Link(filepath.Join(l1, "hl1"), filepath.Join(l1, "hl2"))
	os.Symlink("top", filepath.Join(l1, "s"))
	// overlayfs form, as SingularityCE writes it
	writeFile(t, filepath.Join(l2, "a/sub"), []byte("now a file\n"), 0644)
	writeFile(t, filepath.Join(l2, "b/new"), []byte("new\n"), 0644)
	writeFile(t, filepath.Join(l2, "c/1"), []byte("c/1 v2\n"), 0600)
	writeFile(t, filepath.Join(l2, "n"), []byte("n\n"), 0644)
	os.MkdirAll(filepath.Join(l2, "d"), 0755)
	// AUFS form of the OCI spec
	writeFile(t, filepath.Join(l3, ".wh.hl1"), nil, 0644)
	writeFile(t, filepath.Join(l3, "s/z"), []byte("z\n"), 0644)
	writeFile(t, filepath.Join(l3, "gone/back"), []byte("back\n"), 0644)
	writeFile(t, filepath.Join(l3, "d/.wh..wh..opq"), nil, 0644)
	writeFile(t, filepath.Join(l3, "d/only"), []byte("only\n"), 0644)
	writeFile(t, filepath.Join(l3, "a/.wh.f2"), nil, 0644)
	os.Chmod(filepath.Join(l3, "a"), 0700)

	imgs := []string{filepath.Join(dir, "1.sqfs"), filepath.Join(dir, "2.sqfs"), filepath.Join(dir, "3.sqfs")}
	mklayer(t, l1, imgs[0])
	mklayer(t, l2, imgs[1],
		"top c 644 0 0 0 0", "a/f1 c 644 0 0 0 0", "s c 644 0 0 0 0", "gone c 644 0 0 0 0",
		"b x trusted.overlay.opaque=y")
	mklayer(t, l3, imgs[2])

	exp := filepath.Join(dir, "expected")
	for f, c := range map[string]string{"a/sub": "now a file\n", "b/new": "new\n", "n": "n\n",
		"s/z": "z\n", "gone/back": "back\n", "d/only": "only\n"} {
		writeFile(t, filepath.Join(exp, f), []byte(c), 0644)
	}
	writeFile(t, filepath.Join(exp, "c/1"), []byte("c/1 v2\n"), 0600)
	writeFile(t, filepath.Join(exp, "hl2"), pattern(300000, 5), 0644)
	os.Chmod(filepath.Join(exp, "a"), 0700)
	want := scanTree(t, exp)

	cfg := testConfig()
	sifPath := filepath.Join(dir, "oci.sif")
	writeOCISIF(t, newOCIParts(t, imgs, cfg), sifPath)
	in, data := streamOCI(t, sifPath)
	got, _ := readTar(t, data)
	for k, v := range expectedSingularity(cfg, want) {
		want[k] = v
	}
	compareTrees(t, noMtime(want), noMtime(got))
	if st := in.Merge; st.Layers != 3 || st.Whiteouts != 6 || st.Opaque != 2 {
		t.Errorf("merge stats %+v", st)
	}
	checkContiguous(t, data, true)
}

func TestOCISIFExistingSingularityDir(t *testing.T) {
	requireMksquashfs(t)
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	writeFile(t, filepath.Join(src, ".singularity.d/runscript"), []byte("#!/bin/sh\necho mine\n"), 0700)
	writeFile(t, filepath.Join(src, ".singularity.d/labels.json"), []byte("{}"), 0644)
	os.Symlink("elsewhere", filepath.Join(src, "singularity"))
	img := filepath.Join(dir, "layer.sqfs")
	mksquashfs(t, src, img)
	cfg := testConfig()
	sifPath := filepath.Join(dir, "oci.sif")
	writeOCISIF(t, newOCIParts(t, []string{img}, cfg), sifPath)
	_, data := streamOCI(t, sifPath)
	got, _ := readTar(t, data)
	rs := got[".singularity.d/runscript"]
	if s := sha256.Sum256([]byte(singularity.RunScript(cfg.Config))); rs.sum != hex.EncodeToString(s[:]) || rs.mode != 0755 {
		t.Errorf("runscript not replaced: %+v", rs)
	}
	if _, ok := got[".singularity.d/labels.json"]; !ok {
		t.Errorf("image's own .singularity.d content dropped")
	}
	if got["singularity"].link != "elsewhere" {
		t.Errorf("existing symlink replaced: %+v", got["singularity"])
	}
}

func TestReadOCIErrors(t *testing.T) {
	requireMksquashfs(t)
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	writeFile(t, filepath.Join(src, "f"), []byte("x"), 0644)
	img := filepath.Join(dir, "layer.sqfs")
	mksquashfs(t, src, img)
	cases := map[string]struct {
		mutate func(p *ociParts)
		err    string
	}{
		"missing config": {func(p *ociParts) { p.config = append(p.config, ' ') }, "lacks blob"},
		"layer type": {func(p *ociParts) {
			p.types[0] = "application/vnd.sylabs.image.layer.v1.ext3"
			p.seal()
		}, "unsupported OCI-SIF layer type"},
		"two manifests": {func(p *ociParts) {
			var idx ocispec.Index
			json.Unmarshal(p.index, &idx)
			idx.Manifests = append(idx.Manifests, idx.Manifests[0])
			p.index, _ = json.Marshal(idx)
		}, "2 manifests"},
		"nested index": {func(p *ociParts) {
			var idx ocispec.Index
			json.Unmarshal(p.index, &idx)
			idx.Manifests[0].MediaType = ocispec.MediaTypeImageIndex
			p.index, _ = json.Marshal(idx)
		}, "want an image manifest"},
	}
	for name, c := range cases {
		p := newOCIParts(t, []string{img}, testConfig())
		c.mutate(p)
		sifPath := filepath.Join(dir, "oci.sif")
		writeOCISIF(t, p, sifPath)
		_, err := Identify(context.Background(), sifPath, "", DefaultSourceOptions())
		if err == nil || !strings.Contains(err.Error(), c.err) {
			t.Errorf("%s: got error %v, want %q", name, err, c.err)
		}
	}

	// content no longer matching the digest recorded in the descriptor
	p := newOCIParts(t, []string{img}, testConfig())
	sifPath := filepath.Join(dir, "oci.sif")
	writeOCISIF(t, p, sifPath)
	data, _ := os.ReadFile(sifPath)
	at := bytes.Index(data, p.manifest)
	data[at+len(p.manifest)-1] = ' '
	os.WriteFile(sifPath, data, 0644)
	if _, err := Identify(context.Background(), sifPath, "", DefaultSourceOptions()); err == nil ||
		!strings.Contains(err.Error(), "content has digest") {
		t.Errorf("corrupt manifest: got error %v", err)
	}
}
