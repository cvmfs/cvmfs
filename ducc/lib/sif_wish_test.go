package lib

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/cvmfs/ducc/sif"
)

func drainWishes(r Recipe) []WishFriendly {
	var out []WishFriendly
	for w := range r.Wishes {
		out = append(out, w)
	}
	return out
}

func TestParseRecipeWithoutSIFInput(t *testing.T) {
	r, err := ParseYamlRecipeV1([]byte(`
version: 1
user: cvmfs
cvmfs_repo: unpacked.example.org
input:
  - https://registry.hub.docker.com/library/ubuntu:24.04
`))
	if err != nil {
		t.Fatal(err)
	}
	if len(r.SIFWishes) != 0 || r.NumInputs != 1 {
		t.Errorf("got %d SIF wishes, %d inputs", len(r.SIFWishes), r.NumInputs)
	}
	if got := drainWishes(r); len(got) != 1 {
		t.Errorf("got %d docker wishes, want 1", len(got))
	}
}

func TestParseRecipeSIFInput(t *testing.T) {
	r, err := ParseYamlRecipeV1([]byte(`
version: 1
user: cvmfs
cvmfs_repo: unpacked.example.org
sif_input:
  - source: https://images.example.org/sif/sim-2026.sif
    name: example/sim:2026
  - source: /data/analysis.sif
    name: lhcb/analysis:v3
    digest: sha256:` + strings.Repeat("ab", 32) + `
sif_options:
  contiguous_catalogs: true
  catalog_max_weight: 0
  workers: 3
  cache_size_mib: 16
`))
	if err != nil {
		t.Fatal(err)
	}
	drainWishes(r)
	want := []SIFWish{
		{Source: "https://images.example.org/sif/sim-2026.sif", Name: "example/sim:2026"},
		{Source: "/data/analysis.sif", Name: "lhcb/analysis:v3", Digest: "sha256:" + strings.Repeat("ab", 32)},
	}
	if len(r.SIFWishes) != len(want) || r.SIFWishes[0] != want[0] || r.SIFWishes[1] != want[1] {
		t.Errorf("got %+v, want %+v", r.SIFWishes, want)
	}
	if r.NumInputs != 0 || r.SIFWishes[0].PublicPath() != "sif/example/sim:2026" {
		t.Errorf("NumInputs %d, public path %q", r.NumInputs, r.SIFWishes[0].PublicPath())
	}
	so := r.SIFOptions.streamOptions()
	if !so.ContiguousCatalogs || so.CatalogMaxWeight != 0 || so.CatalogMinWeight != 1000 || so.Workers != 3 {
		t.Errorf("unexpected stream options %+v", so)
	}
	if src := r.SIFOptions.sourceOptions(); src.CacheSize != 16<<20 || src.ChunkSize != 8<<20 {
		t.Errorf("unexpected source options %+v", src)
	}
	def := YamlSIFOptionsV1{}.streamOptions()
	if def.CatalogMaxWeight != 100000 || def.ContiguousCatalogs {
		t.Errorf("unset options must keep the defaults, got %+v", def)
	}
}

func TestParseRecipeSIFInputErrors(t *testing.T) {
	digest := "sha256:" + strings.Repeat("ab", 32)
	cases := map[string]struct{ entries, want string }{
		"no name":      {"  - source: a.sif\n", "public name is required"},
		"absolute":     {"  - {source: a.sif, name: /x}\n", "must be relative"},
		"dotdot":       {"  - {source: a.sif, name: a/../b}\n", "invalid public name"},
		"no source":    {"  - {name: a}\n", "source is required"},
		"bad digest":   {"  - {source: a.sif, name: a, digest: sha256:1234}\n", "invalid digest"},
		"duplicate":    {"  - {source: a.sif, name: a}\n  - {source: b.sif, name: a}\n", `name "a" already used by sif_input[0]`},
		"unknown key":  {"  - {source: a.sif, name: a, digets: " + digest + "}\n", "unknown key(s) digets"},
		"plain string": {"  - a.sif\n", "must be maps"},
	}
	for name, c := range cases {
		r, err := ParseYamlRecipeV1([]byte("version: 1\ncvmfs_repo: r.example.org\nsif_input:\n" + c.entries))
		drainWishes(r)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: got error %v, want %q", name, err, c.want)
		}
	}
	r, err := ParseYamlRecipeV1([]byte("version: 1\ncvmfs_repo: r\nsif_options: {catalog_min_weight: -1}\n"))
	drainWishes(r)
	if err == nil || !strings.Contains(err.Error(), "catalog_min_weight") {
		t.Errorf("negative option: got %v", err)
	}
}

// makeSquashfs builds a bare squashfs image with an Apptainer-like tree;
// bare images need a digest pin, which keeps the test independent of a SIF
// writer.
func makeSquashfs(t *testing.T, content string) string {
	t.Helper()
	if _, err := exec.LookPath("mksquashfs"); err != nil {
		t.Skip("mksquashfs not installed")
	}
	dir := t.TempDir()
	src := filepath.Join(dir, "root")
	for _, d := range []string{".singularity.d", "bin"} {
		if err := os.MkdirAll(filepath.Join(src, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	os.WriteFile(filepath.Join(src, ".singularity.d", "runscript"), []byte("#!/bin/sh\n"), 0o755)
	os.WriteFile(filepath.Join(src, "bin", "hello"), []byte(content), 0o755)
	img := filepath.Join(dir, "img.sqfs")
	out, err := exec.Command("mksquashfs", src, img, "-noappend", "-no-progress", "-no-xattrs").CombinedOutput()
	if err != nil {
		t.Fatalf("mksquashfs: %v\n%s", err, out)
	}
	return img
}

func pin(c string) string { return "sha256:" + strings.Repeat(c, 64) }

func sorted(s []string) []string {
	s = append([]string(nil), s...)
	sort.Strings(s)
	return s
}

func checkSummary(t *testing.T, step string, s ConversionSummary, added, updated, already, failed []string) {
	t.Helper()
	for _, c := range []struct {
		what      string
		got, want []string
	}{{"added", s.Added, added}, {"updated", s.Updated, updated}, {"already", s.AlreadyConverted, already}, {"failed", s.Failed, failed}} {
		if strings.Join(sorted(c.got), ",") != strings.Join(sorted(c.want), ",") {
			t.Errorf("%s: %s = %v, want %v", step, c.what, c.got, c.want)
		}
	}
}

func TestConvertSIFWishesThroughMockRepo(t *testing.T) {
	imgA := makeSquashfs(t, "a")
	imgB := makeSquashfs(t, "b")
	repo := mockCVMFSRepo()
	recipe := Recipe{Repo: repo, SIFWishes: []SIFWish{
		{Source: imgA, Name: "wl/a:1", Digest: pin("1")},
		{Source: filepath.Join(t.TempDir(), "missing.sif"), Name: "wl/missing:1"},
		{Source: imgB, Name: "wl/b:1", Digest: pin("2")},
	}}
	ctx := context.Background()

	s, errs := ConvertSIFWishes(ctx, recipe, false, nil, nil)
	checkSummary(t, "first", s, []string{"sif/wl/a:1", "sif/wl/b:1"}, nil, nil, []string{"sif/wl/missing:1"})
	if len(errs) != 1 || !strings.Contains(errs[0], "missing.sif") {
		t.Errorf("first: errors %v", errs)
	}
	for _, id := range []string{strings.Repeat("1", 64), strings.Repeat("2", 64)} {
		if _, err := os.Stat(filepath.Join(testMockRepo, sif.FlatPath(id), "bin", "hello")); err != nil {
			t.Errorf("image %s not ingested: %v", id, err)
		}
		if _, err := os.Stat(filepath.Join(testMockRepo, sif.InfoPath(id))); err != nil {
			t.Errorf("provenance of %s missing: %v", id, err)
		}
	}

	ignored := func(src string) bool { return strings.HasSuffix(src, "missing.sif") }
	s, errs = ConvertSIFWishes(ctx, recipe, false, ignored, nil)
	checkSummary(t, "unchanged", s, nil, nil, []string{"sif/wl/a:1", "sif/wl/b:1"}, nil)
	if len(errs) != 0 {
		t.Errorf("unchanged: ignored error reported: %v", errs)
	}

	// A new pin for the same name is a new image: ingest and relink.
	recipe.SIFWishes[0].Digest = pin("3")
	s, _ = ConvertSIFWishes(ctx, recipe, false, ignored, nil)
	checkSummary(t, "repinned", s, nil, []string{"sif/wl/a:1"}, []string{"sif/wl/b:1"}, nil)
	if !sif.Published(repo, "wl/a:1", strings.Repeat("3", 64)) {
		t.Error("sif/wl/a:1 not relinked to the new image")
	}

	// The old image is now unreferenced; GC must take its provenance too.
	used, _ := FindAllUsedFlatImages(repo)
	all, _ := FindAllFlatImages(repo)
	usedSet := map[string]bool{}
	for _, u := range used {
		usedSet[u] = true
	}
	var unused []string
	for _, f := range all {
		if !usedSet[f] {
			unused = append(unused, f)
		}
	}
	oldFlat := filepath.Join(testMockRepo, sif.FlatPath(strings.Repeat("1", 64)))
	for _, keep := range []string{"2", "3"} {
		if !usedSet[filepath.Join(testMockRepo, sif.FlatPath(strings.Repeat(keep, 64)))] {
			t.Errorf("image %s linked from sif/ not counted as used: %v", keep, used)
		}
	}
	if !containsImage(unused, oldFlat) {
		t.Fatalf("old image %s not a GC candidate: %v", oldFlat, unused)
	}
	infos := FindSifInfoToDelete(unused)
	if !containsImage(infos, filepath.Join(testMockRepo, sif.InfoPath(strings.Repeat("1", 64)))) {
		t.Errorf("provenance of the old image not scheduled: %v", infos)
	}
	for _, i := range infos {
		if strings.Contains(i, strings.Repeat("3", 64)) || strings.Contains(i, strings.Repeat("2", 64)) {
			t.Errorf("provenance of a used image scheduled: %s", i)
		}
	}

	// --convert-again re-ingests an unchanged image.
	s, _ = ConvertSIFWishes(ctx, Recipe{Repo: repo, SIFWishes: recipe.SIFWishes[2:]}, true, nil, nil)
	checkSummary(t, "force", s, nil, []string{"sif/wl/b:1"}, nil, nil)

	deleted, err := DeleteSIFFromCVMFS(repo, "wl/b:1", true)
	if err != nil || !deleted {
		t.Fatalf("dry-run delete: %v %v", deleted, err)
	}
	if _, err := os.Lstat(filepath.Join(testMockRepo, "sif", "wl", "b:1")); err != nil {
		t.Error("dry run removed the link")
	}
	if deleted, err := DeleteSIFFromCVMFS(repo, "wl", false); err == nil || deleted {
		t.Errorf("deleting the directory sif/wl must fail, got %v %v", deleted, err)
	}
	if deleted, err := DeleteSIFFromCVMFS(repo, "wl/none:1", false); err != nil || deleted {
		t.Errorf("missing link: got %v %v", deleted, err)
	}
}

func TestFindSifInfoToDeleteIgnoresOtherPaths(t *testing.T) {
	id := strings.Repeat("c", 64)
	root := filepath.Join(testMockRepo, "sub")
	os.MkdirAll(filepath.Join(root, ".sifinfo", "cc"), 0o755)
	os.WriteFile(filepath.Join(root, ".sifinfo", "cc", id+".json"), []byte("{}"), 0o644)
	got := FindSifInfoToDelete([]string{
		filepath.Join(root, ".flat", "cc", id),
		filepath.Join(root, ".flat", "dd", id),     // shard mismatch
		filepath.Join(root, ".layers", "cc", id),   // not a flat image
		filepath.Join(root, ".flat", "cc", "c"+id), // no provenance
	})
	want := filepath.Join(root, ".sifinfo", "cc", id+".json")
	if len(got) != 1 || got[0] != want {
		t.Errorf("got %v, want [%s]", got, want)
	}
}
