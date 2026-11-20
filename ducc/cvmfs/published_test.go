package cvmfs

import (
	"bytes"
	"compress/zlib"
	"crypto/md5"
	"database/sql"
	"encoding/binary"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

type fakeEntry struct {
	path  string
	flags int64
	link  string
}

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
			CONSTRAINT pk_catalog PRIMARY KEY (md5path_1, md5path_2))`,
		`CREATE TABLE nested_catalogs (path TEXT, sha1 TEXT, size INTEGER, CONSTRAINT pk PRIMARY KEY (path))`,
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	for _, e := range entries {
		sum := md5.Sum([]byte(e.path))
		if _, err := db.Exec(`INSERT INTO catalog VALUES (?, ?, ?, ?)`,
			int64(binary.LittleEndian.Uint64(sum[:8])), int64(binary.LittleEndian.Uint64(sum[8:])),
			e.flags, e.link); err != nil {
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
	var buf bytes.Buffer
	zw := zlib.NewWriter(&buf)
	zw.Write(raw)
	zw.Close()
	return buf.Bytes()
}

func TestLookupPublished(t *testing.T) {
	const rootHash, flatHash, imgHash, decoyHash = "aa11-rmd160", "bb22-rmd160", "cc33-rmd160", "dd44-rmd160"
	id := "ab" + fmt.Sprintf("%062d", 7)
	objects := map[string][]byte{
		rootHash: buildCatalog(t, []fakeEntry{
			{"", flagDir, ""},
			{"/.flat", flagDir, ""},
			{"/.flat/ab", flagDir | flagNestedMountpoint, ""},
			{"/sif", flagDir, ""},
			{"/sif/img:1", flagLink, "../.flat/ab/" + id},
			{"/a_c", flagDir | flagNestedMountpoint, ""},
			{"/ABC", flagDir | flagNestedMountpoint, ""},
			{"/abc", flagDir, ""},
			{"/abc/x", 4, ""},
		}, map[string]string{"/.flat/ab": flatHash, "/a_c": decoyHash, "/ABC": decoyHash}),
		flatHash: buildCatalog(t, []fakeEntry{
			{"/.flat/ab", flagDir, ""},
			{"/.flat/ab/" + id, flagDir | flagNestedMountpoint, ""},
		}, map[string]string{"/.flat/ab/" + id: imgHash}),
		decoyHash: buildCatalog(t, nil, nil),
	}
	var fetched []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/.cvmfspublished" {
			fmt.Fprintf(w, "C%s\nRdeadbeef\nS42\n--\nsignature\x00\x01", rootHash)
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
		if err != nil || got != want {
			t.Errorf("%s: got %+v, %v; want %+v", path, got, err, want)
		}
	}
	check(".flat/ab/"+id, PublishedEntry{Exists: true, Dir: true, NestedMountpoint: true})
	check(".flat/ab/"+fmt.Sprintf("ab%062d", 8), PublishedEntry{})
	check("sif/img:1", PublishedEntry{Exists: true, Symlink: true, LinkTarget: "../.flat/ab/" + id})
	check("sif/other", PublishedEntry{})
	check("abc/x", PublishedEntry{Exists: true})
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
	if n := len(fetched); n != 2 {
		t.Errorf("catalogs fetched %d times, want 2 (cached by hash): %v", n, fetched)
	}
}
