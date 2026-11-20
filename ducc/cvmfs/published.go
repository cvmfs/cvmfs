package cvmfs

// Lookups in the published revision of a repository, for mountless
// publishers where /cvmfs/<repo> is not mounted. The manifest and the
// catalogs on the path are fetched from CVMFS_STRATUM0 and queried
// directly; catalogs are content addressed, so they are cached by hash.

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"container/list"
	"crypto/md5"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const (
	flagDir              = 1
	flagNestedMountpoint = 2
	flagLink             = 8
)

// PublishedEntry describes a path in the published revision.
type PublishedEntry struct {
	Exists bool
	Dir    bool
	// NestedMountpoint marks the root of a nested catalog, as created by
	// `cvmfs_server ingest --catalog`.
	NestedMountpoint bool
	Symlink          bool
	LinkTarget       string
}

var (
	publishedClient = &http.Client{Timeout: 2 * time.Minute}
	stratum0URLs    sync.Map // repo name -> URL
	catalogs        = newCatalogCache(32)
)

// stratum0URL reads CVMFS_STRATUM0 from the repository's server.conf;
// tests override it.
var stratum0URL = func(repoName string) (string, error) {
	if v, ok := stratum0URLs.Load(repoName); ok {
		return v.(string), nil
	}
	conf, err := os.ReadFile(filepath.Join("/etc/cvmfs/repositories.d", repoName, "server.conf"))
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(conf), "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "CVMFS_STRATUM0="); ok {
			url := strings.TrimRight(strings.Trim(v, `"'`), "/")
			stratum0URLs.Store(repoName, url)
			return url, nil
		}
	}
	return "", fmt.Errorf("no CVMFS_STRATUM0 in server.conf of %s", repoName)
}

// LookupPublished looks up repoPath (relative to the repository, subdir
// prefix applied) in the currently published revision.
func LookupPublished(CVMFSRepo, repoPath string) (PublishedEntry, error) {
	repoName, _ := GetRepoAndSubdir(CVMFSRepo)
	base, err := stratum0URL(repoName)
	if err != nil {
		return PublishedEntry{}, err
	}
	root, err := publishedRootHash(base)
	if err != nil {
		return PublishedEntry{}, err
	}
	path := "/" + PrefixRepoSubdirOnce(CVMFSRepo, repoPath)
	if path == "/" {
		path = ""
	}

	hash := root
	for depth := 0; ; depth++ {
		if depth > 64 {
			return PublishedEntry{}, fmt.Errorf("nested catalogs too deep at %s", path)
		}
		db, err := catalogs.get(base, hash)
		if err != nil {
			return PublishedEntry{}, err
		}
		// Descend while a nested catalog is mounted strictly above path; the
		// mountpoint entry itself is also in the parent catalog.
		var child string
		// substr, not LIKE: LIKE is case-insensitive and treats _ as a wildcard.
		err = db.QueryRow(`SELECT sha1 FROM nested_catalogs
			WHERE substr(?1, 1, length(path) + 1) = path || '/'
			ORDER BY length(path) DESC LIMIT 1`, path).Scan(&child)
		if err == nil {
			hash = child
			continue
		}
		if err != sql.ErrNoRows {
			return PublishedEntry{}, fmt.Errorf("catalog %s: %w", hash, err)
		}
		return lookupEntry(db, path)
	}
}

func lookupEntry(db *sql.DB, path string) (PublishedEntry, error) {
	sum := md5.Sum([]byte(path))
	lo := int64(binary.LittleEndian.Uint64(sum[:8]))
	hi := int64(binary.LittleEndian.Uint64(sum[8:]))
	var flags int64
	var link sql.NullString
	err := db.QueryRow(`SELECT flags, symlink FROM catalog WHERE md5path_1 = ? AND md5path_2 = ?`,
		lo, hi).Scan(&flags, &link)
	if err == sql.ErrNoRows {
		return PublishedEntry{}, nil
	}
	if err != nil {
		return PublishedEntry{}, err
	}
	return PublishedEntry{
		Exists:           true,
		Dir:              flags&flagDir != 0,
		NestedMountpoint: flags&flagNestedMountpoint != 0,
		Symlink:          flags&flagLink != 0,
		LinkTarget:       link.String,
	}, nil
}

// publishedRootHash reads the root catalog hash from .cvmfspublished. The
// signature is not checked: a forged manifest could only make ducc skip or
// repeat work, it is never used to serve content.
func publishedRootHash(base string) (string, error) {
	body, err := fetch(base + "/.cvmfspublished")
	if err != nil {
		return "", err
	}
	sc := bufio.NewScanner(bytes.NewReader(body))
	for sc.Scan() {
		line := sc.Text()
		if line == "--" {
			break
		}
		if strings.HasPrefix(line, "C") && len(line) > 1 {
			return line[1:], nil
		}
	}
	return "", fmt.Errorf("%s/.cvmfspublished has no root catalog hash", base)
}

func fetch(url string) ([]byte, error) {
	resp, err := publishedClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: HTTP %s", url, resp.Status)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 1<<30))
}

// catalogObjectPath maps a hash string such as "ab12...-rmd160" to
// data/ab/12...-rmd160C.
func catalogObjectPath(hash string) (string, error) {
	if len(hash) < 3 || strings.ContainsAny(hash, "/.") {
		return "", fmt.Errorf("invalid catalog hash %q", hash)
	}
	return "data/" + hash[:2] + "/" + hash[2:] + "C", nil
}

// catalogCache keeps decompressed catalogs as unlinked temporary SQLite
// files.
type catalogCache struct {
	mu    sync.Mutex
	max   int
	lru   *list.List
	items map[string]*list.Element
}

type cachedCatalog struct {
	hash string
	db   *sql.DB
}

func newCatalogCache(max int) *catalogCache {
	return &catalogCache{max: max, lru: list.New(), items: map[string]*list.Element{}}
}

func (c *catalogCache) get(base, hash string) (*sql.DB, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[hash]; ok {
		c.lru.MoveToFront(el)
		return el.Value.(*cachedCatalog).db, nil
	}
	obj, err := catalogObjectPath(hash)
	if err != nil {
		return nil, err
	}
	raw, err := fetch(base + "/" + obj)
	if err != nil {
		return nil, err
	}
	data := raw
	if !bytes.HasPrefix(raw, []byte("SQLite format 3")) {
		zr, err := zlib.NewReader(bytes.NewReader(raw))
		if err != nil {
			return nil, fmt.Errorf("catalog %s: %w", hash, err)
		}
		if data, err = io.ReadAll(zr); err != nil {
			return nil, fmt.Errorf("catalog %s: %w", hash, err)
		}
	}
	f, err := os.CreateTemp("", "ducc-catalog-*.db")
	if err != nil {
		return nil, err
	}
	_, err = f.Write(data)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(f.Name())
		return nil, err
	}
	db, err := sql.Open("sqlite", "file:"+f.Name()+"?mode=ro")
	if err == nil {
		// One connection that is never recycled keeps the file open, so it
		// can be unlinked right away and never outlives the process.
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		db.SetConnMaxLifetime(0)
		db.SetConnMaxIdleTime(0)
		err = db.Ping()
	}
	os.Remove(f.Name())
	if err != nil {
		if db != nil {
			db.Close()
		}
		return nil, err
	}
	c.items[hash] = c.lru.PushFront(&cachedCatalog{hash, db})
	for c.lru.Len() > c.max {
		old := c.lru.Back()
		c.lru.Remove(old)
		cc := old.Value.(*cachedCatalog)
		delete(c.items, cc.hash)
		cc.db.Close()
	}
	return db, nil
}
