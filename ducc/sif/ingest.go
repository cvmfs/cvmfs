package sif

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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
	"github.com/cvmfs/ducc/singularity"
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
	OCI            *OCIImage    `json:"oci,omitempty"`
	Merge          *MergeStats  `json:"merge,omitempty"`
	Squashfs       squashInfo   `json:"squashfs"` // lowest layer of an OCI-SIF
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

// ValidateDigest checks an image ID override.
func ValidateDigest(d string) error {
	_, err := normalizeDigest(d)
	return err
}

func FlatPath(id string) string     { return filepath.Join(".flat", id[:2], id) }
func InfoPath(id string) string     { return filepath.Join(".sifinfo", id[:2], id+".json") }
func PublicPath(name string) string { return filepath.Join(PublicPrefix, name) }

// Inspect opens the source and builds the index without touching any
// repository; used by --dry-run and --emit-tar as well.
type Inspected struct {
	Src   Source
	Image *Image
	OCI   *OCIImage // OCI-SIF only
	Tree  *Tree
	Merge *MergeStats // OCI-SIF only
	ID    string
	// IDKind is user-digest, oci-config, source-digest or sif-descriptors.
	IDKind string
}

// DataSize is the size of the squashfs data the image is read from.
func (in *Inspected) DataSize() int64 {
	if in.OCI == nil {
		return in.Image.PartSize
	}
	var n int64
	for _, l := range in.OCI.Layers {
		n += l.Size
	}
	return n
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
	if in.Image.OCI {
		if in.OCI, err = in.Image.ReadOCI(src); err != nil {
			return fail(err)
		}
		in.Image.Arch = in.OCI.Config.Architecture
	}
	switch {
	case digest != "":
		if in.ID, err = normalizeDigest(digest); err != nil {
			return fail(err)
		}
		in.IDKind = "user-digest"
	case in.OCI != nil:
		// Same ID as a registry conversion of this image (GetSingularityPath),
		// so both share one .flat tree.
		if in.ID, err = normalizeDigest(in.OCI.ConfigDigest); err != nil {
			return fail(err)
		}
		in.IDKind = "oci-config"
	case src.KnownDigest() != "":
		in.ID, in.IDKind = src.KnownDigest(), "source-digest"
	case in.Image.Bare:
		return fail(fmt.Errorf("bare squashfs images have no SIF header to derive an ID from; pass --digest"))
	default:
		in.ID, in.IDKind = in.Image.HeaderDigest(), "sif-descriptors"
	}
	return in, nil
}

// BuildIndex opens the squashfs partition, or the layers of an OCI-SIF,
// and reads the directory tree.
func (in *Inspected) BuildIndex(maxEntries int) error {
	if maxEntries <= 0 {
		maxEntries = 10_000_000
	}
	if in.OCI != nil {
		var err error
		in.Tree, in.Merge, err = BuildOCITree(in.Src, in.OCI, maxEntries)
		return err
	}
	fs, err := squashfs.Open(in.Image.Partition(in.Src), in.Image.PartSize)
	if err != nil {
		return err
	}
	ix, err := fs.BuildIndex(maxEntries)
	if err != nil {
		return err
	}
	fs.DropMetadataCache()
	in.Tree = SingleTree(fs, ix)
	return nil
}

// BuildOCITree merges the layers of an OCI-SIF and adds the Singularity
// environment that registry conversions get from `cvmfs_server overlay -c`.
func BuildOCITree(r io.ReaderAt, o *OCIImage, maxEntries int) (*Tree, *MergeStats, error) {
	var layers []*squashfs.FS
	var ixs []*squashfs.Index
	total := 0
	for i, l := range o.Layers {
		fs, err := squashfs.Open(o.Layer(r, i), l.Size)
		if err != nil {
			return nil, nil, fmt.Errorf("layer %s: %w", l.Digest, err)
		}
		ix, err := fs.BuildIndex(maxEntries - total)
		if err != nil {
			return nil, nil, fmt.Errorf("layer %s: %w", l.Digest, err)
		}
		total += len(ix.Entries)
		layers, ixs = append(layers, fs), append(ixs, ix)
	}
	b, err := merge(layers, ixs)
	if err != nil {
		return nil, nil, err
	}
	for _, fs := range layers {
		fs.DropMetadataCache()
	}
	mtime := time.Now()
	if c := o.Config.Created; c != nil && c.Unix() > 0 {
		mtime = *c
	}
	if err := b.addFiles(singularity.Files(o.Config), uint32(mtime.Unix())); err != nil {
		return nil, nil, err
	}
	t, st, err := b.build(layers)
	if err != nil {
		return nil, nil, err
	}
	return t, &st, nil
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
	var target string
	if cvmfs.MountlessPublishing() {
		e, err := cvmfs.LookupPublished(repo, link)
		if err != nil || !e.Symlink {
			return false
		}
		target = e.LinkTarget
	} else {
		var err error
		if target, err = os.Readlink(abs); err != nil {
			return false
		}
	}
	if !filepath.IsAbs(target) {
		target = filepath.Join(filepath.Dir(abs), target)
	}
	return filepath.Clean(target) == filepath.Join("/", "cvmfs", repo, flat)
}

func flatComplete(repo, flat string) bool {
	if cvmfs.MountlessPublishing() {
		// The image root is a nested catalog created by one ingest, so it is
		// published complete or not at all. Checking .singularity.d would
		// download the image's root catalog on every check.
		e, err := cvmfs.LookupPublished(repo, flat)
		return err == nil && e.NestedMountpoint
	}
	_, err := os.Stat(filepath.Join("/", "cvmfs", repo, flat, ".singularity.d"))
	return err == nil
}

// Published reports whether image id is complete in the repository and
// sif/<name> points at it, i.e. whether Convert would be a no-op.
func Published(repo, name, id string) bool {
	return flatComplete(repo, FlatPath(id)) && symlinkPointsTo(repo, PublicPath(name), FlatPath(id))
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
		fields := log.Fields{"entries": len(in.Tree.Index.Entries), "id_kind": in.IDKind}
		if in.Merge != nil {
			fields["layers"], fields["whiteouts"], fields["hidden"] = in.Merge.Layers, in.Merge.Whiteouts, in.Merge.Hidden
		}
		logger.WithFields(fields).Info("SIF image indexed")
		if !hasSingularityDir(in.Tree.Index) && !o.AllowMissingSingularityDir {
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
	// A gateway lease needs an existing parent directory, as for
	// .layers/<xx>; a catalog per prefix also keeps .flat's catalog small.
	if err := cvmfs.CreateCatalogIntoDirWithLogger(logger, o.Repo, filepath.Dir(res.FlatPath)); err != nil {
		return fmt.Errorf("creating %s: %w", filepath.Dir(res.FlatPath), err)
	}
	target := cvmfs.PrefixRepoSubdirOnce(o.Repo, res.FlatPath)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	pr, pw := io.Pipe()
	streamErr := make(chan error, 1)
	sopts := o.Stream
	sopts.Logger = logger
	go func() {
		st, err := StreamTree(ctx, pw, in.Tree, sopts)
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
		// The stream only sees a closed pipe when ingest gives up first.
		if serr != nil && serr != context.Canceled && !errors.Is(serr, io.ErrClosedPipe) {
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
	if n := in.DataSize(); n > 0 {
		ratio = float64(src.BytesFetched) / float64(n)
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
	// The image ID is not always a hash of the whole source (OCI-SIFs use
	// the config digest), so verify against the source's own digest.
	want := in.Src.KnownDigest()
	if want == "" && in.IDKind == "user-digest" {
		want = in.ID
	}
	if want == "" {
		return fmt.Errorf("--verify-digest needs a content digest (oras:// source or --digest)")
	}
	if fs, ok := in.Src.(*fileSource); ok && fs.remove && fs.digest == want {
		return nil // checked while downloading
	}
	t := time.Now()
	h := sha256.New()
	r := io.NewSectionReader(in.Src, 0, in.Src.Size())
	if _, err := io.Copy(h, &ctxReader{ctx, r}); err != nil {
		return fmt.Errorf("verifying digest: %w", err)
	}
	sum := hex.EncodeToString(h.Sum(nil))
	if sum != want {
		// The tree stays unlinked and is removed by the next flat GC.
		return fmt.Errorf("source has sha256 %s, expected %s; image not linked", sum, want)
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
	sb := in.Tree.Layers[0].Superblock()
	p := Provenance{
		Schema: 1, Source: o.Source, SourceIdentity: in.Src.Identity(),
		ImageID: in.ID, IDKind: in.IDKind, OCI: in.OCI, Merge: in.Merge,
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
