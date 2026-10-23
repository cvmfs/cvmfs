package cvmfs

// Mountless publishing support.
//
// A mountless gateway publisher (cvmfs_server mkfs -P / connect-gw -P) has no
// FUSE union mount, so the transaction+publish workflow used by
// PublishToCVMFS / CreateSymlinkIntoCVMFS / CreateCatalogIntoDir does not work.
// Everything those helpers do, however, can be expressed as a
// `cvmfs_server ingest` of a small tar stream, which works in both mounted and
// mountless setups (the server auto-detects the mount state).
//
// These helpers are only used when mountless publishing is enabled
// (CVMFS_DUCC_MOUNTLESS_PUBLISHING or SetMountlessPublishing); the default
// path is unchanged for mounted publishers.

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	log "github.com/sirupsen/logrus"

	l "github.com/cvmfs/ducc/log"
)

// mountlessPublishing routes repo writes through `cvmfs_server ingest` instead
// of transaction+publish.  Enabled by env for convenience and by
// SetMountlessPublishing from the CLI.
var mountlessPublishing = os.Getenv("CVMFS_DUCC_MOUNTLESS_PUBLISHING") != ""

// SetMountlessPublishing toggles ingest-based writes for mountless gateway
// publishers.  Call once at start-up (e.g. from a --mountless flag).
func SetMountlessPublishing(v bool) { mountlessPublishing = v }

// MountlessPublishing reports whether ingest-based writes are in effect.
func MountlessPublishing() bool { return mountlessPublishing }

// ingestTarStreamWithLogger pipes a tar archive (produced by fill) into
//
//	cvmfs_server ingest [extraOpts...] -t - -b <baseDir> <repo>
//
// baseDir is the repository-relative path (subdir prefix included, as for
// layer ingestion).  The tar is generated on the fly, so no temporary files
// are needed.
func ingestTarStreamWithLogger(logger *log.Entry, CVMFSRepo, baseDir string,
	fill func(tw *tar.Writer) error, extraOpts ...string) error {
	pr, pw := io.Pipe()
	go func() {
		tw := tar.NewWriter(pw)
		err := fill(tw)
		if err == nil {
			err = tw.Close()
		}
		// Propagate a fill/close error to the reader so the ingest aborts
		// instead of consuming a truncated archive.
		pw.CloseWithError(err)
	}()

	opts := append([]string{}, extraOpts...)
	opts = append(opts, "-t", "-", "-b", baseDir)
	return IngestWithLogger(logger, CVMFSRepo, pr, opts...)
}

// publishToCVMFSMountless materializes a regular file or a directory tree at
// repoPath (repository path without the /cvmfs/<repo>/ prefix) via ingest.
func publishToCVMFSMountless(logger *log.Entry, CVMFSRepo, repoPath, target string) error {
	logger = l.Ensure(logger)
	st, err := os.Stat(target)
	if err != nil {
		logger.WithField("error", err).Error("mountless publish: cannot stat target")
		return err
	}

	// Repository-relative destination, with the subdir prefix applied exactly
	// once (matches the convention used for layer ingestion).
	dest := PrefixRepoSubdirOnce(CVMFSRepo, repoPath)

	if st.Mode().IsDir() {
		// Replace any previous tree: only --fast-delete is available mountless
		// (regular --delete needs the rdonly mount for traversal).  Best effort
		// — the path may not exist yet.
		_ = IngestFastDeleteWithLogger(logger, CVMFSRepo, dest)
		return ingestTarStreamWithLogger(logger, CVMFSRepo, dest,
			func(tw *tar.Writer) error { return writeDirTree(tw, target) })
	}

	if !st.Mode().IsRegular() {
		return fmt.Errorf("Trying to ingest neither a file nor a directory")
	}

	// A single regular file: extract one named member into the parent dir.
	// The tarball engine adds-or-replaces the entry, so no delete is needed.
	baseDir := filepath.Dir(dest)
	if baseDir == "." {
		baseDir = ""
	}
	name := filepath.Base(dest)
	return ingestTarStreamWithLogger(logger, CVMFSRepo, baseDir,
		func(tw *tar.Writer) error { return writeRegularFile(tw, name, target, st) })
}

// createSymlinkMountless creates (or replaces) a symlink at repoLink pointing
// to linkTarget (the verbatim link contents).
func createSymlinkMountless(logger *log.Entry, CVMFSRepo, repoLink, linkTarget string) error {
	logger = l.Ensure(logger)
	dest := PrefixRepoSubdirOnce(CVMFSRepo, repoLink)
	baseDir := filepath.Dir(dest)
	if baseDir == "." {
		baseDir = ""
	}
	name := filepath.Base(dest)
	return ingestTarStreamWithLogger(logger, CVMFSRepo, baseDir,
		func(tw *tar.Writer) error {
			return tw.WriteHeader(&tar.Header{
				Typeflag: tar.TypeSymlink,
				Name:     name,
				Linkname: linkTarget,
				Mode:     0777,
				Uid:      os.Getuid(),
				Gid:      os.Getgid(),
				ModTime:  time.Now(),
			})
		})
}

// writeRegularFile writes target as a single tar member called name.
func writeRegularFile(tw *tar.Writer, name, target string, st os.FileInfo) error {
	hdr := &tar.Header{
		Typeflag: tar.TypeReg,
		Name:     name,
		Mode:     int64(st.Mode().Perm()),
		Size:     st.Size(),
		Uid:      os.Getuid(),
		Gid:      os.Getgid(),
		ModTime:  st.ModTime(),
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}
	f, err := os.Open(target)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(tw, f)
	return err
}

// writeDirTree walks root and writes its contents into the tar with paths
// relative to root (so they land directly under the ingest base_dir).
func writeDirTree(tw *tar.Writer, root string) error {
	return filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil // the base_dir itself is created by the ingest
		}

		var link string
		if info.Mode()&os.ModeSymlink != 0 {
			if link, err = os.Readlink(path); err != nil {
				return err
			}
		}
		hdr, err := tar.FileInfoHeader(info, link)
		if err != nil {
			return err
		}
		hdr.Name = rel
		hdr.Uid = os.Getuid()
		hdr.Gid = os.Getgid()
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(tw, f)
		return err
	})
}
