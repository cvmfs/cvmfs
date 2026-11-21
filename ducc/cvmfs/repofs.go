package cvmfs

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// Repository view for read-only checks on /cvmfs/<repo>/... paths. On a
// mounted publisher these are the os functions; with mountless publishing
// they answer from the published catalogs (LookupPublished), so existence
// checks keep working without a mount.

func splitRepoPath(abs string) (repo, rel string, ok bool) {
	rest, found := strings.CutPrefix(filepath.Clean(abs), "/cvmfs/")
	if !found {
		return "", "", false
	}
	repo, rel, _ = strings.Cut(rest, "/")
	return repo, rel, repo != ""
}

type publishedInfo struct {
	name string
	e    PublishedEntry
}

func (i publishedInfo) Name() string { return i.name }
func (i publishedInfo) Size() int64  { return i.e.Size }
func (i publishedInfo) Mode() fs.FileMode {
	switch {
	case i.e.Dir:
		return fs.ModeDir | 0755
	case i.e.Symlink:
		return fs.ModeSymlink | 0777
	case i.e.Regular:
		return 0644
	}
	return fs.ModeIrregular
}
func (i publishedInfo) ModTime() time.Time { return time.Time{} }
func (i publishedInfo) IsDir() bool        { return i.e.Dir }
func (i publishedInfo) Sys() interface{}   { return nil }

func publishedLstat(op, abs string) (PublishedEntry, error) {
	repo, rel, ok := splitRepoPath(abs)
	if !ok {
		return PublishedEntry{}, &fs.PathError{Op: op, Path: abs, Err: fs.ErrInvalid}
	}
	e, err := LookupPublished(repo, rel)
	if err != nil {
		return e, &fs.PathError{Op: op, Path: abs, Err: err}
	}
	if !e.Exists {
		return e, &fs.PathError{Op: op, Path: abs, Err: fs.ErrNotExist}
	}
	return e, nil
}

// RepoLstat is os.Lstat on a /cvmfs/<repo>/... path.
func RepoLstat(abs string) (fs.FileInfo, error) {
	if !MountlessPublishing() {
		return os.Lstat(abs)
	}
	e, err := publishedLstat("lstat", abs)
	if err != nil {
		return nil, err
	}
	return publishedInfo{filepath.Base(abs), e}, nil
}

var variantExpr = regexp.MustCompile(`\$\(([A-Za-z0-9_]+)(:-([^)]*))?\)`)

// RepoStat is os.Stat on a /cvmfs/<repo>/... path. Without a mount, variant
// symlinks resolve to their default, as on a client without the variable.
func RepoStat(abs string) (fs.FileInfo, error) {
	if !MountlessPublishing() {
		return os.Stat(abs)
	}
	_, e, err := publishedResolve(abs)
	if err != nil {
		return nil, err
	}
	return publishedInfo{filepath.Base(abs), e}, nil
}

// publishedResolve follows symlinks, as the kernel would on a mount.
func publishedResolve(abs string) (string, PublishedEntry, error) {
	path := abs
	for hops := 0; hops < 16; hops++ {
		e, err := publishedLstat("stat", path)
		if err != nil {
			return "", e, err
		}
		if !e.Symlink {
			return path, e, nil
		}
		target := variantExpr.ReplaceAllString(e.LinkTarget, "$3")
		if !filepath.IsAbs(target) {
			target = filepath.Join(filepath.Dir(path), target)
		}
		path = filepath.Clean(target)
	}
	return "", PublishedEntry{}, &fs.PathError{Op: "stat", Path: abs, Err: fs.ErrInvalid}
}

// RepoSameFile is os.SameFile for the results of RepoStat on a and b.
// Without a mount there are no inode numbers, so it compares where the two
// paths resolve to.
func RepoSameFile(a, b string, ai, bi fs.FileInfo) bool {
	if !MountlessPublishing() {
		return os.SameFile(ai, bi)
	}
	ra, _, errA := publishedResolve(a)
	rb, _, errB := publishedResolve(b)
	return errA == nil && errB == nil && ra == rb
}

// RepoReadlink is os.Readlink on a /cvmfs/<repo>/... path.
func RepoReadlink(abs string) (string, error) {
	if !MountlessPublishing() {
		return os.Readlink(abs)
	}
	e, err := publishedLstat("readlink", abs)
	if err != nil {
		return "", err
	}
	if !e.Symlink {
		return "", &fs.PathError{Op: "readlink", Path: abs, Err: fs.ErrInvalid}
	}
	return e.LinkTarget, nil
}

// RepoReadFile is os.ReadFile for small files on a /cvmfs/<repo>/... path.
func RepoReadFile(abs string) ([]byte, error) {
	if !MountlessPublishing() {
		return os.ReadFile(abs)
	}
	repo, rel, ok := splitRepoPath(abs)
	if !ok {
		return nil, &fs.PathError{Op: "read", Path: abs, Err: fs.ErrInvalid}
	}
	data, err := ReadPublishedFile(repo, rel)
	if err != nil {
		return nil, &fs.PathError{Op: "read", Path: abs, Err: err}
	}
	return data, nil
}
