package cvmfs

import (
	"bytes"
	"compress/zlib"
	"crypto/md5"
	"crypto/sha1"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type fakeEntry struct {
	path  string
	flags int64
	link  string
	data  []byte // regular file content, stored as a zlib sha1 object
}

func zlibBytes(b []byte) []byte {
	var buf bytes.Buffer
	zw := zlib.NewWriter(&buf)
	zw.Write(b)
	zw.Close()
	return buf.Bytes()
}

// fakeObjects collects content objects referenced by fake catalogs.
var fakeObjects = map[string][]byte{}

// buildCatalog writes a minimal catalog database and returns it
// zlib-compressed, as stored in the repository.
func buildCatalog(t *testing.T, entries []fakeEntry, nested map[string]string) []byte {
	t.Helper()
	file := filepath.Join(t.TempDir(), "c.db")
	db, err := sql.Open("sqlite", file)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`CREATE TABLE catalog (md5path_1 INTEGER, md5path_2 INTEGER, flags INTEGER, symlink TEXT,
			size INTEGER, hash BLOB,
			CONSTRAINT pk_catalog PRIMARY KEY (md5path_1, md5path_2))`,
		`CREATE TABLE nested_catalogs (path TEXT, sha1 TEXT, size INTEGER, CONSTRAINT pk PRIMARY KEY (path))`,
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	for _, e := range entries {
		sum := md5.Sum([]byte(e.path))
		var hash []byte
		if e.data != nil {
			h := sha1.Sum(e.data)
			hash = h[:]
			hx := hex.EncodeToString(hash)
			fakeObjects["data/"+hx[:2]+"/"+hx[2:]] = zlibBytes(e.data)
		}
		if _, err := db.Exec(`INSERT INTO catalog VALUES (?, ?, ?, ?, ?, ?)`,
			int64(binary.LittleEndian.Uint64(sum[:8])), int64(binary.LittleEndian.Uint64(sum[8:])),
			e.flags, e.link, len(e.data), hash); err != nil {
			t.Fatal(err)
		}
	}
	for p, h := range nested {
		if _, err := db.Exec(`INSERT INTO nested_catalogs VALUES (?, ?, 0)`, p, h); err != nil {
			t.Fatal(err)
		}
	}
	db.Close()
	raw, _ := os.ReadFile(file)
	return zlibBytes(raw)
}

func TestLookupPublished(t *testing.T) {
	const rootHash, flatHash, imgHash, decoyHash = "aa11-rmd160", "bb22-rmd160", "cc33-rmd160", "dd44-rmd160"
	id := "ab" + fmt.Sprintf("%062d", 7)
	objects := map[string][]byte{
		rootHash: buildCatalog(t, []fakeEntry{
			{"", flagDir, "", nil},
			{"/.flat", flagDir, "", nil},
			{"/.flat/ab", flagDir | flagNestedMountpoint, "", nil},
			{"/sif", flagDir, "", nil},
			{"/sif/img:1", flagLink, "../.flat/ab/" + id, nil},
			{"/a_c", flagDir | flagNestedMountpoint, "", nil},
			{"/ABC", flagDir | flagNestedMountpoint, "", nil},
			{"/abc", flagDir, "", nil},
			{"/abc/x", 4, "", nil},
			{"/meta", flagDir, "", nil},
			{"/meta/manifest.json", flagFile, "", []byte(`{"config":{"digest":"sha256:x"}}`)},
			{"/.multiarch", flagDir, "", nil},
			{"/.multiarch/amd64", flagDir, "", nil},
			{"/sif/variant", flagLink, "../.multiarch/$(CVMFS_ARCH:-amd64)", nil},
		}, map[string]string{"/.flat/ab": flatHash, "/a_c": decoyHash, "/ABC": decoyHash}),
		flatHash: buildCatalog(t, []fakeEntry{
			{"/.flat/ab", flagDir, "", nil},
			{"/.flat/ab/" + id, flagDir | flagNestedMountpoint, "", nil},
		}, map[string]string{"/.flat/ab/" + id: imgHash}),
		decoyHash: buildCatalog(t, nil, nil),
	}
	var fetched []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/.cvmfspublished" {
			fmt.Fprintf(w, "C%s\nRdeadbeef\nS42\n--\nsignature\x00\x01", rootHash)
			return
		}
		if data, ok := fakeObjects[strings.TrimPrefix(r.URL.Path, "/")]; ok {
			w.Write(data)
			return
		}
		for h, data := range objects {
			if p, _ := catalogObjectPath(h); r.URL.Path == "/"+p {
				fetched = append(fetched, h)
				w.Write(data)
				return
			}
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()
	catalogs = newCatalogCache(32)
	old := stratum0URL
	stratum0URL = func(string) (string, error) { return srv.URL, nil }
	defer func() { stratum0URL = old }()

	check := func(path string, want PublishedEntry) {
		t.Helper()
		got, err := LookupPublished("repo.example.org", path)
		if err != nil || got.Exists != want.Exists || got.Dir != want.Dir ||
			got.NestedMountpoint != want.NestedMountpoint || got.Symlink != want.Symlink ||
			got.LinkTarget != want.LinkTarget || got.Regular != want.Regular {
			t.Errorf("%s: got %+v, %v; want %+v", path, got, err, want)
		}
	}
	check(".flat/ab/"+id, PublishedEntry{Exists: true, Dir: true, NestedMountpoint: true})
	check(".flat/ab/"+fmt.Sprintf("ab%062d", 8), PublishedEntry{})
	check("sif/img:1", PublishedEntry{Exists: true, Symlink: true, LinkTarget: "../.flat/ab/" + id})
	check("sif/other", PublishedEntry{})
	check("abc/x", PublishedEntry{Exists: true, Regular: true})
	for _, h := range fetched {
		if h == imgHash || h == decoyHash {
			t.Errorf("fetched catalog %s that does not lie on the looked-up paths", h)
		}
	}
	// subdir repositories prefix the path once
	check("abc", PublishedEntry{Exists: true, Dir: true})
	got, err := LookupPublished("repo.example.org/abc", "x")
	if err != nil || !got.Exists {
		t.Errorf("subdir lookup: %+v, %v", got, err)
	}
	if left, _ := filepath.Glob(filepath.Join(os.TempDir(), "ducc-catalog-*")); len(left) != 0 {
		t.Errorf("catalog temp files left behind: %v", left)
	}
	data, err := ReadPublishedFile("repo.example.org", "meta/manifest.json")
	if err != nil || string(data) != `{"config":{"digest":"sha256:x"}}` {
		t.Errorf("ReadPublishedFile: %q, %v", data, err)
	}
	if _, err := ReadPublishedFile("repo.example.org", "meta/missing"); !os.IsNotExist(err) {
		t.Errorf("missing file: %v", err)
	}

	SetMountlessPublishing(true)
	defer SetMountlessPublishing(false)
	if fi, err := RepoStat("/cvmfs/repo.example.org/sif/variant"); err != nil || !fi.IsDir() {
		t.Errorf("RepoStat through variant symlink: %v, %v", fi, err)
	}
	if _, err := RepoStat("/cvmfs/repo.example.org/sif/missing"); !os.IsNotExist(err) {
		t.Errorf("RepoStat missing: %v", err)
	}
	if fi, err := RepoLstat("/cvmfs/repo.example.org/sif/img:1"); err != nil || fi.Mode()&os.ModeSymlink == 0 {
		t.Errorf("RepoLstat symlink: %v, %v", fi, err)
	}
	if fi, err := RepoStat("/cvmfs/repo.example.org/meta/manifest.json"); err != nil || !fi.Mode().IsRegular() {
		t.Errorf("RepoStat regular file: %v, %v", fi, err)
	}
	if b, err := RepoReadFile("/cvmfs/repo.example.org/meta/manifest.json"); err != nil || len(b) == 0 {
		t.Errorf("RepoReadFile: %q, %v", b, err)
	}
	if target, err := RepoReadlink("/cvmfs/repo.example.org/sif/img:1"); err != nil || target != "../.flat/ab/"+id {
		t.Errorf("RepoReadlink: %q, %v", target, err)
	}

	if n := len(fetched); n != 2 {
		t.Errorf("catalogs fetched %d times, want 2 (cached by hash): %v", n, fetched)
	}
}
