package sif

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/cvmfs/ducc/cvmfs"
	l "github.com/cvmfs/ducc/log"
	"github.com/cvmfs/ducc/notification"
	"github.com/cvmfs/ducc/sif/squashfs"
)

const PublicPrefix = "sif"

type ConvertOptions struct {
	Source string
	Repo   string
	// Name is the public path below sif/; required unless the source
	// implies one.
	Name   string
	Digest string // image ID override, "sha256:HEX" or "HEX"
	Force  bool
	// AllowMissingSingularityDir publishes images without /.singularity.d.
	AllowMissingSingularityDir bool
	// VerifyDigest hashes the whole source after ingest and links the image
	// only if it matches the digest used as image ID.
	VerifyDigest bool
	MaxEntries   int
	Stream       StreamOptions
	SourceOpts   SourceOptions
	Notification *notification.Notification
	Logger       *log.Entry
}

type Result struct {
	ImageID    string
	IDKind     string
	FlatPath   string
	PublicPath string
	Skipped    bool // image already present, only the symlink was updated
	Stream     StreamStats
	Source     SourceStats
}

// Provenance is stored at .sifinfo/<xx>/<id>.json.
type Provenance struct {
	Schema         int          `json:"schema"`
	Source         string       `json:"source"`
	SourceIdentity string       `json:"source_identity"`
	ImageID        string       `json:"image_id"`
	IDKind         string       `json:"id_kind"`
	SIF            *sifInfo     `json:"sif,omitempty"`
	Squashfs       squashInfo   `json:"squashfs"`
	Stats          StreamStats  `json:"stats"`
	Fetch          *SourceStats `json:"fetch,omitempty"`
	IngestedAt     time.Time    `json:"ingested_at"`
}

type sifInfo struct {
	UUID        string       `json:"uuid"`
	Created     time.Time    `json:"created"`
	Arch        string       `json:"arch"`
	Signed      bool         `json:"signed"`
	Descriptors []Descriptor `json:"descriptors"`
}

type squashInfo struct {
	Compressor string `json:"compressor"`
	BlockSize  uint32 `json:"block_size"`
	Inodes     uint32 `json:"inodes"`
	BytesUsed  uint64 `json:"bytes_used"`
}

var hexID = regexp.MustCompile(`^[0-9a-f]{64}$`)

func normalizeDigest(d string) (string, error) {
	d = strings.TrimPrefix(strings.ToLower(d), "sha256:")
	if !hexID.MatchString(d) {
		return "", fmt.Errorf("invalid digest %q: want sha256:<64 hex chars>", d)
	}
	return d, nil
}

// ValidateName checks a public name relative to sif/.
func ValidateName(name string) error {
	if name == "" {
		return fmt.Errorf("a public name is required for this source (--name)")
	}
	if strings.HasPrefix(name, "/") {
		return fmt.Errorf("public name %q must be relative", name)
	}
	for _, c := range strings.Split(name, "/") {
		if c == "" || c == "." || c == ".." {
			return fmt.Errorf("invalid public name %q", name)
		}
	}
	return nil
}

func FlatPath(id string) string     { return filepath.Join(".flat", id[:2], id) }
func InfoPath(id string) string     { return filepath.Join(".sifinfo", id[:2], id+".json") }
func PublicPath(name string) string { return filepath.Join(PublicPrefix, name) }

// Inspect opens the source and builds the index without touching any
// repository; used by --dry-run and --emit-tar as well.
type Inspected struct {
	Src    Source
	Image  *Image
	FS     *squashfs.FS
	Index  *squashfs.Index
	ID     string
	IDKind string
}

func Inspect(ctx context.Context, source, digest string, maxEntries int, sopts SourceOptions) (*Inspected, error) {
	in, err := Identify(ctx, source, digest, sopts)
	if err != nil {
		return nil, err
	}
	if err := in.BuildIndex(maxEntries); err != nil {
		in.Src.Close()
		return nil, err
	}
	return in, nil
}

// Identify opens the source and determines the image ID from the SIF
// header alone (a few small reads), so an unchanged image can be skipped
// without reading the squashfs metadata.
func Identify(ctx context.Context, source, digest string, sopts SourceOptions) (*Inspected, error) {
	src, err := OpenSource(ctx, source, sopts)
	if err != nil {
		return nil, err
	}
	in := &Inspected{Src: src}
	fail := func(err error) (*Inspected, error) {
		src.Close()
		return nil, err
	}
	if in.Image, err = Parse(src, src.Size()); err != nil {
		return fail(err)
	}
	switch {
	case digest != "":
		if in.ID, err = normalizeDigest(digest); err != nil {
			return fail(err)
		}
		in.IDKind = "user-digest"
	case src.KnownDigest() != "":
		in.ID, in.IDKind = src.KnownDigest(), "source-digest"
	case in.Image.Bare:
		return fail(fmt.Errorf("bare squashfs images have no SIF header to derive an ID from; pass --digest"))
	default:
		in.ID, in.IDKind = in.Image.HeaderDigest(), "sif-descriptors"
	}
	return in, nil
}

// BuildIndex opens the squashfs partition and reads its directory tree.
func (in *Inspected) BuildIndex(maxEntries int) (err error) {
	if in.FS, err = squashfs.Open(in.Image.Partition(in.Src), in.Image.PartSize); err != nil {
		return err
	}
	if maxEntries <= 0 {
		maxEntries = 10_000_000
	}
	if in.Index, err = in.FS.BuildIndex(maxEntries); err != nil {
		return err
	}
	in.FS.DropMetadataCache()
	return nil
}

func hasSingularityDir(ix *squashfs.Index) bool {
	for i := 1; i < len(ix.Entries); i++ {
		e := &ix.Entries[i]
		if e.Parent == 0 && e.Name == ".singularity.d" && e.Kind == squashfs.KindDir {
			return true
		}
	}
	return false
}

// symlinkPointsTo avoids publishing a new revision for an unchanged link.
// Without a mounted repository it reports false and the link is rewritten.
func symlinkPointsTo(repo, link, flat string) bool {
	abs := filepath.Join("/", "cvmfs", repo, link)
	target, err := os.Readlink(abs)
	if err != nil {
		return false
	}
	if !filepath.IsAbs(target) {
		target = filepath.Join(filepath.Dir(abs), target)
	}
	return filepath.Clean(target) == filepath.Join("/", "cvmfs", repo, flat)
}

func flatComplete(repo, flat string) bool {
	_, err := os.Stat(filepath.Join("/", "cvmfs", repo, flat, ".singularity.d"))
	return err == nil
}

// Convert publishes the image's root filesystem at .flat/<xx>/<id> with a
// single streamed ingest, then points sif/<name> at it.
func Convert(ctx context.Context, o ConvertOptions) (res Result, err error) {
	logger := l.Ensure(o.Logger).WithFields(log.Fields{"source": o.Source, "repo": o.Repo})
	if o.Name == "" {
		o.Name = DefaultName(o.Source)
	}
	if err = ValidateName(o.Name); err != nil {
		return
	}
	n := o.Notification
	if n == nil {
		discard := notification.NewNotificationServiceFromWriter(io.Discard)
		n = notification.NewNotification(&discard)
	}
	n = n.AddField("source", o.Source).AddField("repo", o.Repo).AddId()
	t0 := time.Now()
	n.Action("start_sif_ingest").Send()
	defer func() {
		n.Elapsed(t0).Action("end_sif_ingest").
			AddField("image_id", res.ImageID).
			AddField("skipped", fmt.Sprint(res.Skipped)).
			AddField("entries", fmt.Sprint(res.Stream.Entries)).
			AddField("bytes_fetched", fmt.Sprint(res.Source.BytesFetched)).
			AddField("xattrs_dropped", fmt.Sprint(res.Stream.XattrsDropped)).
			SizeBytes(res.Stream.Bytes).
			Error(err).Send()
	}()

	o.SourceOpts.Logger = logger
	in, err := Identify(ctx, o.Source, o.Digest, o.SourceOpts)
	if err != nil {
		return
	}
	defer in.Src.Close()
	res.ImageID, res.IDKind = in.ID, in.IDKind
	res.FlatPath, res.PublicPath = FlatPath(in.ID), PublicPath(o.Name)
	logger = logger.WithFields(log.Fields{"image_id": in.ID, "flat": res.FlatPath, "public": res.PublicPath})

	if !o.Force && flatComplete(o.Repo, res.FlatPath) {
		logger.Info("image already ingested")
		res.Skipped = true
	} else {
		if err = in.BuildIndex(o.MaxEntries); err != nil {
			return
		}
		logger.WithFields(log.Fields{"entries": len(in.Index.Entries), "id_kind": in.IDKind}).Info("SIF image indexed")
		if !hasSingularityDir(in.Index) && !o.AllowMissingSingularityDir {
			err = fmt.Errorf("image has no /.singularity.d; not an Apptainer image? (override with --allow-missing-singularity-d)")
			return
		}
		if o.Force {
			// Ingesting over an existing tree keeps the old metadata of
			// directories that already exist, so start from scratch.
			if derr := cvmfs.IngestFastDeleteWithLogger(logger, o.Repo, res.FlatPath); derr != nil {
				logger.WithField("error", derr).Debug("no previous tree to delete")
			}
		}
		if err = ingest(ctx, logger, o, in, &res); err != nil {
			return
		}
	}
	res.Source = in.Src.Stats()

	if o.VerifyDigest && !res.Skipped {
		if err = verifyDigest(ctx, logger, in); err != nil {
			return
		}
	}

	if !res.Skipped {
		if perr := writeProvenance(logger, o, in, &res); perr != nil {
			logger.WithField("error", perr).Warn("could not write provenance file")
		}
	}
	if symlinkPointsTo(o.Repo, res.PublicPath, res.FlatPath) {
		logger.Info("public symlink already up to date")
	} else if err = cvmfs.CreateSymlinkIntoCVMFSWithLogger(logger, o.Repo, res.PublicPath, res.FlatPath); err != nil {
		err = fmt.Errorf("creating symlink %s: %w", res.PublicPath, err)
		return
	}
	logger.Info("SIF image published")
	return
}

func ingest(ctx context.Context, logger *log.Entry, o ConvertOptions, in *Inspected, res *Result) error {
	if err := cvmfs.CreateCatalogIntoDirWithLogger(logger, o.Repo, ".flat"); err != nil {
		logger.WithField("error", err).Warn("could not create catalog in .flat")
	}
	target := cvmfs.PrefixRepoSubdirOnce(o.Repo, res.FlatPath)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	pr, pw := io.Pipe()
	streamErr := make(chan error, 1)
	sopts := o.Stream
	sopts.Logger = logger
	go func() {
		st, err := Stream(ctx, pw, in.FS, in.Index, sopts)
		res.Stream = st
		// Closing with the error makes ingest see a truncated archive and
		// abort instead of committing a partial tree.
		pw.CloseWithError(err)
		streamErr <- err
	}()

	t := time.Now()
	// Without --keep-ownership every file would be owned by the repository
	// owner, losing e.g. root:tty on setgid binaries.
	args := []string{"--catalog", "--keep-ownership"}
	if o.Stream.ContiguousCatalogs {
		args = append(args, "--contiguous-catalogs")
	}
	ingestErr := cvmfs.IngestWithLogger(logger, o.Repo, pr, append(args, "-t", "-", "-b", target)...)
	pr.CloseWithError(fmt.Errorf("ingest finished"))
	cancel()
	serr := <-streamErr
	if serr != nil && ingestErr == nil {
		// Should not happen: a truncated tar fails the ingest. Be safe.
		ingestErr = fmt.Errorf("tar stream failed after ingest succeeded: %w", serr)
	}
	if ingestErr != nil {
		if serr != nil && serr != context.Canceled {
			ingestErr = fmt.Errorf("%w (stream: %v)", ingestErr, serr)
		}
		logger.WithField("error", ingestErr).Error("SIF ingest failed")
		if derr := cvmfs.IngestFastDeleteWithLogger(logger, o.Repo, res.FlatPath); derr != nil {
			logger.WithField("error", derr).Warn("cleanup of failed ingest path failed")
		}
		return ingestErr
	}
	st := res.Stream
	src := in.Src.Stats()
	ratio := 0.0
	if in.Image.PartSize > 0 {
		ratio = float64(src.BytesFetched) / float64(in.Image.PartSize)
	}
	logger.WithFields(log.Fields{
		"entries": st.Entries, "files": st.Files, "hardlinks": st.Hardlinks,
		"bytes": st.Bytes, "catalogs": st.Catalogs, "xattrs_dropped": st.XattrsDropped,
		"sockets_dropped": st.SocketsDropped, "bytes_fetched": src.BytesFetched,
		"fetch_ratio": fmt.Sprintf("%.3f", ratio), "elapsed": time.Since(t).Round(time.Millisecond),
	}).Info("SIF ingest finished")
	if st.XattrsDropped > 0 {
		logger.WithFields(log.Fields{"xattrs": st.XattrsDropped, "inodes": st.XattrInodes}).
			Warn("extended attributes were dropped; the tarball ingest does not support them")
	}
	return nil
}

func verifyDigest(ctx context.Context, logger *log.Entry, in *Inspected) error {
	if in.IDKind == "sif-descriptors" {
		return fmt.Errorf("--verify-digest needs a content digest (oras:// source or --digest)")
	}
	if fs, ok := in.Src.(*fileSource); ok && fs.remove && fs.digest == in.ID {
		return nil // checked while downloading
	}
	t := time.Now()
	h := sha256.New()
	r := io.NewSectionReader(in.Src, 0, in.Src.Size())
	if _, err := io.Copy(h, &ctxReader{ctx, r}); err != nil {
		return fmt.Errorf("verifying digest: %w", err)
	}
	sum := hex.EncodeToString(h.Sum(nil))
	if sum != in.ID {
		// The tree stays unlinked and is removed by the next flat GC.
		return fmt.Errorf("source has sha256 %s, expected %s; image not linked", sum, in.ID)
	}
	logger.WithField("elapsed", time.Since(t).Round(time.Millisecond)).Info("source digest verified")
	return nil
}

type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c *ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

func NewProvenance(o ConvertOptions, in *Inspected, res *Result) Provenance {
	sb := in.FS.Superblock()
	p := Provenance{
		Schema: 1, Source: o.Source, SourceIdentity: in.Src.Identity(),
		ImageID: in.ID, IDKind: in.IDKind,
		Squashfs: squashInfo{sb.Compressor.String(), sb.BlockSize, sb.InodeCount, sb.BytesUsed},
		Stats:    res.Stream, IngestedAt: time.Now().UTC(),
	}
	if !in.Image.Bare {
		p.SIF = &sifInfo{in.Image.UUID, in.Image.Created, in.Image.Arch, in.Image.Signed, in.Image.Descs}
	}
	fetch := res.Source
	p.Fetch = &fetch
	return p
}

func writeProvenance(logger *log.Entry, o ConvertOptions, in *Inspected, res *Result) error {
	data, err := json.MarshalIndent(NewProvenance(o, in, res), "", "  ")
	if err != nil {
		return err
	}
	return cvmfs.WriteDataToCvmfsWithLogger(logger, o.Repo, InfoPath(in.ID), append(data, '\n'))
}
